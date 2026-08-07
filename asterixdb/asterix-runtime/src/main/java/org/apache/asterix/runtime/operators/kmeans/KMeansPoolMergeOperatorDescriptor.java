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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ initialization loop — <b>Op4 PoolMerge</b>: the single-node draw-union of one
 * oversampling round. It consumes each Sample (Op3) partition's drawn candidates plus a per-partition
 * {@link KMeansLoopIO#KIND_END} marker (delivered via a concurrent M-to-1), and once it has seen the end markers
 * from all {@code nParticipants} partitions for a round it emits that round's <b>global union</b> of draws in a
 * deterministic order (partition ASC, then per-partition draw sequence ASC), followed by one end marker
 * (broadcast to the Release operators, Op5).
 * <p>
 * The deterministic union order is what keeps every partition's pool run file byte-identical, so all partitions
 * agree on phi and the draws each subsequent round. Draw vectors are decoded and buffered as {@code double[]}
 * (the input frame buffers are transient), then re-emitted. Because the loop is globally serialized, at most one
 * round is live in the accumulator at a time (emitted and removed before the next round arrives).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY k-means|| init loop: single-node draw-union (Op4)")
public class KMeansPoolMergeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    // Number of Sample (Op3) partitions whose end markers must arrive before a round's union is complete.
    private final int nParticipants;

    public KMeansPoolMergeOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            int nParticipants) {
        super(spec, 1, 1);
        this.nParticipants = nParticipants;
        outRecDescs[0] = recDesc; // DRAW_RD shape, in == out
    }

    /** One buffered draw awaiting the round's barrier: its origin partition, per-partition seq, and vector. */
    private static final class Draw {
        private final int part;
        private final int seq;
        private final double[] vec;

        private Draw(int part, int seq, double[] vec) {
            this.part = part;
            this.seq = seq;
            this.vec = vec;
        }
    }

    private static final Comparator<Draw> UNION_ORDER =
            Comparator.comparingInt((Draw d) -> d.part).thenComparingInt(d -> d.seq);

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
            private final FrameTupleReference tuple = new FrameTupleReference();
            private final Map<Integer, List<Draw>> drawsByRound = new HashMap<>();
            private final Map<Integer, Integer> endsByRound = new HashMap<>();
            private FrameTupleAppender appender;
            private ArrayTupleBuilder tb;

            @Override
            public void open() throws HyracksDataException {
                appender = new FrameTupleAppender(new VSizeFrame(ctx));
                tb = new ArrayTupleBuilder(5);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tuple.reset(accessor, i);
                    int round = IntegerPointable.getInteger(tuple.getFieldData(0), tuple.getFieldStart(0));
                    int part = IntegerPointable.getInteger(tuple.getFieldData(1), tuple.getFieldStart(1));
                    int seq = IntegerPointable.getInteger(tuple.getFieldData(2), tuple.getFieldStart(2));
                    int kind = IntegerPointable.getInteger(tuple.getFieldData(3), tuple.getFieldStart(3));
                    if (kind == KMeansLoopIO.KIND_END) {
                        int ends = endsByRound.merge(round, 1, Integer::sum);
                        if (ends == nParticipants) {
                            emitUnion(round);
                            drawsByRound.remove(round);
                            endsByRound.remove(round);
                        }
                    } else {
                        double[] vec = KMeansLoopIO.readRawVector(tuple.getFieldData(4), tuple.getFieldStart(4),
                                tuple.getFieldLength(4));
                        drawsByRound.computeIfAbsent(round, k -> new ArrayList<>()).add(new Draw(part, seq, vec));
                    }
                }
            }

            private void emitUnion(int round) throws HyracksDataException {
                // A round may draw nothing (e.g. the pool already covers every point -> phi = 0): still emit the
                // end marker so Release wakes Cost for the next round. (getOrDefault(List.of()) would be immutable
                // -> sort throws; guard on null instead.)
                List<Draw> draws = drawsByRound.get(round);
                if (draws != null) {
                    draws.sort(UNION_ORDER);
                    for (Draw d : draws) {
                        emitDraw(round, d.part, d.seq, d.vec);
                    }
                }
                emitEnd(round);
                appender.write(writer, true);
                writer.flush();
            }

            private void emitDraw(int round, int part, int seq, double[] vec) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, round);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, part);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, seq);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_DRAW);
                KMeansLoopIO.writeRawVector(tb, vec);
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            }

            private void emitEnd(int round) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, round);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, 0);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, 0);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_END);
                KMeansLoopIO.writeRawVector(tb, new double[] { 0.0d }); // ignored for end markers
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
