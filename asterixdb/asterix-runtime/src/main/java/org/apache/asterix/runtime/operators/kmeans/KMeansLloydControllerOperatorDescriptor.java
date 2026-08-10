/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.operators.kmeans;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ Lloyd loop — the loop head: materialize this partition's vectors once, then run every
 * refinement iteration against them without leaving the operator.
 * <p>
 * An iteration is one assignment pass: each resident vector is charged to its nearest current centroid, and the
 * partition emits one {@code (count, sum)} partial per centroid that attracted anything. The reduce that turns
 * those partials into the next centroid set is a separate single-node operator, so the iteration crosses the
 * network twice — out as partials, back as centroids — on ordinary pipelined connectors. Iteration is paced by
 * this partition's permit: the head emits, then parks until the tail has published the new centroids.
 * <p>
 * Iterating inside one operator keeps the plan a fixed size: the vectors are written to a single run file and
 * re-streamed each round, so neither the graph nor the materialization grows with the iteration count. It also
 * makes a data-dependent count expressible, since the graph is the same however many times it runs, though the
 * count passed in today is a constant.
 * <p>
 * Vectors never move between nodes: only the per-centroid partials (O(k · dim) per partition) and the centroid
 * set travel, both independent of the input size.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class KMeansLloydControllerOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int STORE_VECTORS_ACTIVITY_ID = 0;
    private static final int STORE_CENTROIDS_ACTIVITY_ID = 1;
    private static final int LLOYD_LOOP_ACTIVITY_ID = 2;

    private static final int OUT_CENTROIDS = 0; // the final centroid set (plain vectors), downstream
    private static final int OUT_PARTIALS = 1; // per-iteration (count, sum) partials -> CentroidMerge

    /** A parked head must not wait forever for a tail that a failed sibling will never reach. */
    private static final long PERMIT_TIMEOUT_MINUTES = 30L;

    private final String loopKey;
    private final int vectorColumn; // vector column in input 0
    private final int centroidColumn; // vector column in input 1 (the initial centroids)
    private final int iterations;

    public KMeansLloydControllerOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor centroidRecDesc,
            RecordDescriptor partialRecDesc, String loopKey, int vectorColumn, int centroidColumn, int iterations) {
        super(spec, 2, 2);
        this.loopKey = loopKey;
        this.vectorColumn = vectorColumn;
        this.centroidColumn = centroidColumn;
        this.iterations = iterations;
        outRecDescs[OUT_CENTROIDS] = centroidRecDesc;
        outRecDescs[OUT_PARTIALS] = partialRecDesc;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        StoreVectorsActivity storeVectors = new StoreVectorsActivity(new ActivityId(odId, STORE_VECTORS_ACTIVITY_ID));
        StoreCentroidsActivity storeCentroids =
                new StoreCentroidsActivity(new ActivityId(odId, STORE_CENTROIDS_ACTIVITY_ID));
        LloydLoopActivity loop = new LloydLoopActivity(new ActivityId(odId, LLOYD_LOOP_ACTIVITY_ID));

        builder.addActivity(this, storeVectors);
        builder.addSourceEdge(0, storeVectors, 0);
        builder.addActivity(this, storeCentroids);
        builder.addSourceEdge(1, storeCentroids, 0);
        builder.addActivity(this, loop);
        builder.addTargetEdge(OUT_CENTROIDS, loop, OUT_CENTROIDS);
        builder.addTargetEdge(OUT_PARTIALS, loop, OUT_PARTIALS);

        builder.addBlockingEdge(storeVectors, loop);
        builder.addBlockingEdge(storeCentroids, loop);
    }

    /** Materializes the partition's resident vectors into a raw-double run file for repeated re-streaming. */
    private final class StoreVectorsActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private StoreVectorsActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
                private final FrameTupleReference tuple = new FrameTupleReference();
                private final KMeansVectorCodec.ListVectorDecoder decoder = new KMeansVectorCodec.ListVectorDecoder();
                private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
                private MaterializerTaskState state;
                private VSizeFrame frame;
                private FrameTupleAppender appender;

                @Override
                public void open() throws HyracksDataException {
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.setId(LoopControlState.vectorsStateId(loopKey, partition));
                    state.open(ctx);
                    frame = new VSizeFrame(ctx);
                    appender = new FrameTupleAppender(frame);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        tuple.reset(accessor, i);
                        double[] vec = decoder.decode(tuple, vectorColumn);
                        tb.reset();
                        KMeansLoopIO.writeRawVector(tb, vec);
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            flushToState();
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw HyracksDataException
                                        .create(new IllegalStateException("stored vector exceeds a frame"));
                            }
                        }
                    }
                }

                private void flushToState() throws HyracksDataException {
                    if (appender.getTupleCount() > 0) {
                        state.appendFrame(appender.getBuffer());
                        appender.reset(frame, true);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    flushToState();
                    // Fully written; readers open their own independent handles via createReader.
                    state.close();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }

    /**
     * Seeds the loop: reads the (broadcast, hence complete) initial centroid set into this partition's store and
     * registers the control state, so the co-located tail can rendezvous even before the loop body starts.
     */
    private final class StoreCentroidsActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private StoreCentroidsActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
                private final FrameTupleReference tuple = new FrameTupleReference();
                private final KMeansVectorCodec.ListVectorDecoder decoder = new KMeansVectorCodec.ListVectorDecoder();
                private final java.util.List<double[]> initial = new java.util.ArrayList<>();
                private LoopControlState control;

                @Override
                public void open() throws HyracksDataException {
                    control = new LoopControlState(ctx.getJobletContext().getJobId(),
                            LoopControlState.controlStateId(loopKey, partition));
                    control.setId(LoopControlState.controlStateId(loopKey, partition));
                    ctx.setStateObject(control);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        tuple.reset(accessor, i);
                        initial.add(decoder.decode(tuple, centroidColumn));
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    control.getCentroids().put(initial);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }

    /** The inline loop: a 0-input, 2-output source behind both store blocking edges. */
    private final class LloydLoopActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private LloydLoopActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractOperatorNodePushable() {
                private final IFrameWriter[] writers = new IFrameWriter[2];

                @Override
                public int getInputArity() {
                    return 0;
                }

                @Override
                public IFrameWriter getInputFrameWriter(int index) {
                    throw new IllegalStateException();
                }

                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                    writers[index] = writer;
                }

                @Override
                public void initialize() throws HyracksDataException {
                    final IFrameWriter centroidWriter = writers[OUT_CENTROIDS];
                    final IFrameWriter partialWriter = writers[OUT_PARTIALS];
                    centroidWriter.open();
                    partialWriter.open();
                    try {
                        LoopControlState control = (LoopControlState) LoopControlState.await(ctx::getStateObject,
                                LoopControlState.controlStateId(loopKey, partition));
                        MaterializerTaskState vectorState = (MaterializerTaskState) LoopControlState
                                .await(ctx::getStateObject, LoopControlState.vectorsStateId(loopKey, partition));
                        runLoop(control, vectorState, partialWriter);
                        emitFinalCentroids(control, centroidWriter, partition);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        centroidWriter.fail();
                        partialWriter.fail();
                        throw HyracksDataException.create(e);
                    } catch (Exception e) {
                        centroidWriter.fail();
                        partialWriter.fail();
                        throw HyracksDataException.create(e);
                    } finally {
                        centroidWriter.close();
                        partialWriter.close();
                    }
                }

                private void runLoop(LoopControlState control, MaterializerTaskState vectorState,
                        IFrameWriter partialWriter) throws HyracksDataException, InterruptedException {
                    FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(6);
                    for (int it = 0; it < iterations; it++) {
                        final List<double[]> centroids = control.getCentroids().get();
                        final long[] counts = new long[centroids.size()];
                        final double[][] sums = new double[centroids.size()][];
                        KMeansLoopIO.streamRawVectors(vectorState, ctx, vec -> {
                            int bestIdx = -1;
                            double best = Double.POSITIVE_INFINITY;
                            for (int i = 0; i < centroids.size(); i++) {
                                double d = VectorDistanceCalculation.euclideanSquared(vec, centroids.get(i));
                                // Strict <: ties resolve to the first centroid, like nearest-centroid.
                                if (d < best) {
                                    best = d;
                                    bestIdx = i;
                                }
                            }
                            if (bestIdx >= 0 && !Double.isNaN(best)) {
                                counts[bestIdx]++;
                                double[] sum = sums[bestIdx];
                                if (sum == null) {
                                    sum = new double[vec.length];
                                    sums[bestIdx] = sum;
                                }
                                for (int d = 0; d < Math.min(sum.length, vec.length); d++) {
                                    sum[d] += vec[d];
                                }
                            }
                        });
                        for (int i = 0; i < centroids.size(); i++) {
                            if (counts[i] > 0) {
                                emitPartial(partialWriter, appender, tb, it, i, counts[i], sums[i]);
                            }
                        }
                        emitEnd(partialWriter, appender, tb, it);
                        appender.write(partialWriter, true);
                        partialWriter.flush();
                        // Park until the tail has published this iteration's centroid set.
                        if (!control.getPermit().tryAcquire(PERMIT_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                            throw HyracksDataException
                                    .create(new IllegalStateException("kmeans Lloyd loop: permit not released within "
                                            + PERMIT_TIMEOUT_MINUTES + " min (a loop partition may have failed)"));
                        }
                    }
                }

                private void emitPartial(IFrameWriter partialWriter, FrameTupleAppender appender, ArrayTupleBuilder tb,
                        int iter, int seq, long count, double[] sum) throws HyracksDataException {
                    tb.reset();
                    tb.addField(IntegerSerializerDeserializer.INSTANCE, iter);
                    tb.addField(IntegerSerializerDeserializer.INSTANCE, partition);
                    tb.addField(IntegerSerializerDeserializer.INSTANCE, seq);
                    tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_DRAW);
                    tb.addField(DoubleSerializerDeserializer.INSTANCE, (double) count);
                    KMeansLoopIO.writeRawVector(tb, sum);
                    // One iteration emits one partial per non-empty centroid, so the batch is O(k * dim) and
                    // outgrows a frame well before k gets large; flush and carry on rather than treating a full
                    // frame as an error.
                    FrameUtils.appendToWriter(partialWriter, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                            tb.getSize());
                }

                private void emitEnd(IFrameWriter partialWriter, FrameTupleAppender appender, ArrayTupleBuilder tb,
                        int iter) throws HyracksDataException {
                    tb.reset();
                    tb.addField(IntegerSerializerDeserializer.INSTANCE, iter);
                    tb.addField(IntegerSerializerDeserializer.INSTANCE, partition);
                    tb.addField(IntegerSerializerDeserializer.INSTANCE, 0);
                    tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_END);
                    tb.addField(DoubleSerializerDeserializer.INSTANCE, 0.0d);
                    KMeansLoopIO.writeRawVector(tb, new double[] { 0.0d }); // ignored for end markers
                    FrameUtils.appendToWriter(partialWriter, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                            tb.getSize());
                }

                /**
                 * The loop's result. Every partition holds the same set (it was broadcast), so one partition
                 * speaks — matching the single-node reduce this replaces, whose output likewise had one origin.
                 */
                private void emitFinalCentroids(LoopControlState control, IFrameWriter centroidWriter, int partition)
                        throws HyracksDataException {
                    if (partition != 0) {
                        return;
                    }
                    KMeansVectorCodec.PoolEnvelopeWriter out =
                            new KMeansVectorCodec.PoolEnvelopeWriter(ctx, centroidWriter);
                    for (double[] c : control.getCentroids().get()) {
                        out.plainVector(c);
                    }
                    out.flush();
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                }
            };
        }
    }

}
