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
 * CLUSTER BY Route B (multi-NC systolic exact-init loop) — <b>Op1 Cost / Controller</b>: the loop head, the
 * registered descriptor for the {@code OVERSAMPLE_LOOP} logical operator (so the builder wires the vectors+seed
 * inputs here and the parent WEIGH reads the final pool from here), and the fork of the systolic sub-graph.
 * <p>
 * Three activities:
 * <ul>
 * <li><b>StoreVectors</b> (input 0, sink): decodes each resident vector once (ordered list -> {@code double[]})
 * and materializes the per-partition <b>vector run file</b> as raw doubles ({@link KMeansLoopIO#POOL_RD}) — no
 * REPLICATE, decode-once.</li>
 * <li><b>StoreSeed</b> (input 1, sink; the broadcast seed): decodes the seed into the per-partition <b>pool run
 * file</b> ({@code pool[0]}), and creates + registers this partition's {@link LoopControlState} (permit) and the
 * pool run file, so the co-located Sample/Release (Op3/Op5) can find them.</li>
 * <li><b>CostLoop</b> (a 0-input, 2-output source behind both blocking edges): runs the inline loop. Each round
 * reads {@code pool[r]} and streams the vector run file to a local potential {@code localSigma}, emits
 * {@code {round, localSigma}} on <b>output 1</b> (to PhiMerge), then {@code permit.acquire()} — waiting for
 * Release to append the round's global draws and release. After {@code loopRounds}, partition 0 reads the final
 * pool and emits it as {@link KMeansVectorCodec.PoolEnvelopeWriter KIND_POOL envelopes} on <b>output 0</b> (to
 * WEIGH). Output 0 is idle during the loop, so the blocking WEIGH cannot back-pressure the iteration.</li>
 * </ul>
 * The loop is acyclic in the job graph — Release's feedback to CostLoop is the shared permit + pool run file, not
 * a data edge. Same algorithm/draws as the tower (per-round/per-partition seed in Sample); this operator only
 * changes how the rounds are executed. Single-node vs multi-node is irrelevant here — this sub-graph works on any
 * topology (the co-located Op1/Op3/Op5 share an NC's joblet state; the merges are single-node).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY Route B: Cost/Controller loop head (Op1), 2-output source over 2 Store activities")
public class KMeansCostControllerOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int STORE_VECTORS_ACTIVITY_ID = 0;
    private static final int STORE_SEED_ACTIVITY_ID = 1;
    private static final int COST_LOOP_ACTIVITY_ID = 2;

    private static final int OUT_POOL = 0; // final pool -> WEIGH (KIND_POOL envelopes)
    private static final int OUT_SIGMA = 1; // per-round local potential -> PhiMerge (SCALAR_RD)

    // A parked CostLoop should never wait forever for a Release that a failed sibling will never send.
    private static final long PERMIT_TIMEOUT_MINUTES = 30L;

    private final String loopKey;
    private final int vectorColumn; // vector column in input 0
    private final int seedColumn; // vector column in input 1 (the seed)
    private final int loopRounds; // N oversampling rounds

    public KMeansCostControllerOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor poolEnvelopeRecDesc, RecordDescriptor sigmaRecDesc, String loopKey, int vectorColumn,
            int seedColumn, int loopRounds) {
        super(spec, 2, 2);
        this.loopKey = loopKey;
        this.vectorColumn = vectorColumn;
        this.seedColumn = seedColumn;
        this.loopRounds = loopRounds;
        outRecDescs[OUT_POOL] = poolEnvelopeRecDesc;
        outRecDescs[OUT_SIGMA] = sigmaRecDesc;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        StoreActivity storeVectors = new StoreActivity(new ActivityId(odId, STORE_VECTORS_ACTIVITY_ID), true);
        StoreActivity storeSeed = new StoreActivity(new ActivityId(odId, STORE_SEED_ACTIVITY_ID), false);
        CostLoopActivity costLoop = new CostLoopActivity(new ActivityId(odId, COST_LOOP_ACTIVITY_ID));

        builder.addActivity(this, storeVectors);
        builder.addSourceEdge(0, storeVectors, 0);
        builder.addActivity(this, storeSeed);
        builder.addSourceEdge(1, storeSeed, 0);
        builder.addActivity(this, costLoop);
        builder.addTargetEdge(OUT_POOL, costLoop, OUT_POOL);
        builder.addTargetEdge(OUT_SIGMA, costLoop, OUT_SIGMA);

        builder.addBlockingEdge(storeVectors, costLoop);
        builder.addBlockingEdge(storeSeed, costLoop);
    }

    /**
     * Materializes one input stream into a per-partition raw-double run file. {@code vectors=true} stores input 0
     * under the vectors id; {@code vectors=false} stores the seed as {@code pool[0]} under the pool id and also
     * creates + registers this partition's {@link LoopControlState}.
     */
    private final class StoreActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private final boolean vectors;

        private StoreActivity(ActivityId id, boolean vectors) {
            super(id);
            this.vectors = vectors;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final int column = vectors ? vectorColumn : seedColumn;
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
                    state.setId(vectors ? LoopControlState.vectorsStateId(loopKey, partition)
                            : LoopControlState.poolStateId(loopKey, partition));
                    state.open(ctx);
                    frame = new VSizeFrame(ctx);
                    appender = new FrameTupleAppender(frame);
                    if (!vectors) {
                        // Register the loop control (permit) as soon as the pool file exists, so the co-located
                        // Sample/Release can rendezvous even before CostLoop starts.
                        LoopControlState control = new LoopControlState(ctx.getJobletContext().getJobId(),
                                LoopControlState.controlStateId(loopKey, partition));
                        control.setId(LoopControlState.controlStateId(loopKey, partition));
                        ctx.setStateObject(control);
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        tuple.reset(accessor, i);
                        double[] vec = decoder.decode(tuple, column);
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
                    // Do NOT close the pool writer: Release appends to it across the loop. The vector writer is
                    // fully written, but readers open their own handles, so we leave both open to be reclaimed
                    // when the joblet completes. Register so the co-located loop operators can find it.
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }

    /** The inline loop: a 0-input, 2-output source behind both Store blocking edges. */
    private final class CostLoopActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private CostLoopActivity(ActivityId id) {
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
                    final IFrameWriter poolWriter = writers[OUT_POOL];
                    final IFrameWriter sigmaWriter = writers[OUT_SIGMA];
                    poolWriter.open();
                    sigmaWriter.open();
                    try {
                        LoopControlState control = (LoopControlState) LoopControlState.await(ctx::getStateObject,
                                LoopControlState.controlStateId(loopKey, partition));
                        MaterializerTaskState poolState = (MaterializerTaskState) LoopControlState
                                .await(ctx::getStateObject, LoopControlState.poolStateId(loopKey, partition));
                        MaterializerTaskState vectorState = (MaterializerTaskState) LoopControlState
                                .await(ctx::getStateObject, LoopControlState.vectorsStateId(loopKey, partition));
                        runLoop(control, poolState, vectorState, sigmaWriter);
                        emitFinalPool(poolState, poolWriter);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        poolWriter.fail();
                        sigmaWriter.fail();
                        throw HyracksDataException.create(e);
                    } catch (Exception e) {
                        poolWriter.fail();
                        sigmaWriter.fail();
                        throw HyracksDataException.create(e);
                    } finally {
                        poolWriter.close();
                        sigmaWriter.close();
                    }
                }

                private void runLoop(LoopControlState control, MaterializerTaskState poolState,
                        MaterializerTaskState vectorState, IFrameWriter sigmaWriter)
                        throws HyracksDataException, InterruptedException {
                    FrameTupleAppender sigmaAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
                    for (int r = 0; r < loopRounds; r++) {
                        final List<double[]> pool = KMeansLoopIO.readAllRawVectors(poolState, ctx); // pool[r]
                        final double[] localSum = { 0.0d };
                        KMeansLoopIO.streamRawVectors(vectorState, ctx, vec -> {
                            double best = Double.POSITIVE_INFINITY;
                            for (double[] c : pool) {
                                double d = VectorDistanceCalculation.euclideanSquared(vec, c);
                                if (d < best) {
                                    best = d;
                                }
                            }
                            if (!Double.isNaN(best) && best != Double.POSITIVE_INFINITY) {
                                localSum[0] += best;
                            }
                        });
                        tb.reset();
                        tb.addField(IntegerSerializerDeserializer.INSTANCE, r);
                        tb.addField(DoubleSerializerDeserializer.INSTANCE, localSum[0]);
                        if (!sigmaAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            throw HyracksDataException.create(new IllegalStateException("sigma tuple exceeds a frame"));
                        }
                        sigmaAppender.write(sigmaWriter, true); // write + clear the frame for the next round
                        sigmaWriter.flush(); // push this round's localSigma so PhiMerge can proceed
                        // Wait for Release to append round r's global draws (pool[r] -> pool[r+1]) and release.
                        if (!control.getPermit().tryAcquire(PERMIT_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                            throw HyracksDataException.create(
                                    new IllegalStateException("kmeans systolic loop: permit not released within "
                                            + PERMIT_TIMEOUT_MINUTES + " min (a loop partition may have failed)"));
                        }
                    }
                }

                private void emitFinalPool(MaterializerTaskState poolState, IFrameWriter poolWriter)
                        throws HyracksDataException {
                    // The pool is identical on every partition; partition 0 emits it and WEIGH broadcasts it back.
                    if (partition != 0) {
                        return;
                    }
                    KMeansVectorCodec.PoolEnvelopeWriter envelope =
                            new KMeansVectorCodec.PoolEnvelopeWriter(ctx, poolWriter);
                    List<double[]> finalPool = KMeansLoopIO.readAllRawVectors(poolState, ctx);
                    for (int i = 0; i < finalPool.size(); i++) {
                        envelope.poolMember(i, finalPool.get(i));
                    }
                    envelope.flush();
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                }
            };
        }
    }
}
