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
import java.util.Random;

import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ initialization loop — <b>Op3 Sample</b>: the per-partition Bernoulli draw of
 * one oversampling round. Driven by the {@code {round, phi}} frames broadcast from PhiMerge (Op2). For each round
 * it re-reads this partition's shared pool run file (= {@code pool[r]}; Release has not yet appended round r's
 * draws) and streams the shared resident-vector run file, drawing each vector x independently with probability
 * {@code p_x = l * d^2(x, pool) / phi} using a per-round, per-partition seed
 * {@code (seedBase + r) * 1000003 + partition}, so a run is reproducible and independent of partition
 * count. Survivors are emitted as {@link KMeansLoopIO#KIND_DRAW} frames {@code {round, part, seq, vec}}
 * to PoolMerge (Op4), followed by one {@link KMeansLoopIO#KIND_END} marker so Op4's per-round barrier
 * can fire.
 * <p>
 * The pool and vector run files are looked up from joblet state (created by the co-located Cost operator, Op1)
 * with a bounded wait. Points already covered by the pool (d^2 = 0) are never re-drawn, and a non-positive phi
 * (pool already covers everything) yields no draws, as in the paper.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
public class KMeansSampleOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final String loopKey;
    private final int oversamplingCount; // l = oversampling factor * k
    private final long seedBase;

    public KMeansSampleOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor drawRecDesc,
            String loopKey, int oversamplingCount, long seedBase) {
        super(spec, 1, 1);
        this.loopKey = loopKey;
        this.oversamplingCount = oversamplingCount;
        this.seedBase = seedBase;
        outRecDescs[0] = drawRecDesc; // DRAW_RD
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
            private final FrameTupleReference tuple = new FrameTupleReference();
            private final ArrayTupleBuilder tb = new ArrayTupleBuilder(5);
            private FrameTupleAppender appender;
            private MaterializerTaskState poolState;
            private MaterializerTaskState vectorState;

            @Override
            public void open() throws HyracksDataException {
                try {
                    poolState = (MaterializerTaskState) LoopControlState.await(ctx::getStateObject,
                            LoopControlState.poolStateId(loopKey, partition));
                    vectorState = (MaterializerTaskState) LoopControlState.await(ctx::getStateObject,
                            LoopControlState.vectorsStateId(loopKey, partition));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
                appender = new FrameTupleAppender(new VSizeFrame(ctx));
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tuple.reset(accessor, i);
                    int round = IntegerPointable.getInteger(tuple.getFieldData(0), tuple.getFieldStart(0));
                    double phi = DoublePointable.getDouble(tuple.getFieldData(1), tuple.getFieldStart(1));
                    sampleRound(round, phi);
                }
            }

            private void sampleRound(int round, double phi) throws HyracksDataException {
                final List<double[]> pool = KMeansLoopIO.readAllRawVectors(poolState, ctx); // = pool[round]
                final Random rng = new Random((seedBase + round) * 1000003L + partition);
                final double l = oversamplingCount;
                final int[] seq = { 0 };
                if (phi > 0.0d) {
                    KMeansLoopIO.streamRawVectors(vectorState, ctx, vec -> {
                        double best = Double.POSITIVE_INFINITY;
                        for (double[] c : pool) {
                            double d = VectorDistanceCalculation.euclideanSquared(vec, c);
                            if (d < best) {
                                best = d;
                            }
                        }
                        // p_x = 0 for points already in the pool (paper never re-draws them).
                        if (Double.isNaN(best) || best == Double.POSITIVE_INFINITY || best <= 0.0d) {
                            return;
                        }
                        if (rng.nextDouble() < l * best / phi) {
                            emitDraw(round, seq[0]++, vec);
                        }
                    });
                }
                emitEnd(round);
                appender.write(writer, true);
                writer.flush();
            }

            private void emitDraw(int round, int seq, double[] vec) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, round);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, partition);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, seq);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_DRAW);
                KMeansLoopIO.writeRawVector(tb, vec);
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            }

            private void emitEnd(int round) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, round);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, partition);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, 0);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_END);
                KMeansLoopIO.writeRawVector(tb, new double[] { 0.0d });
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                writer.close();
            }
        };
    }
}
