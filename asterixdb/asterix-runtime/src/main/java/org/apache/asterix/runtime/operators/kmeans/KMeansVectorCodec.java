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

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorListDecoder;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY Route B (multi-NC systolic exact-init loop) — codec bridging the loop's boundaries to the shipped
 * CLUSTER BY formats, used only by the Cost/Controller operator (Op1). Two pieces, both kept byte-compatible with
 * {@code KMeansStageRuntime} (the WEIGH / merge Score stages):
 * <ul>
 * <li>{@link ListVectorDecoder} — decodes an input vector column (an ordered list of doubles) into a
 * {@code double[]}. Op1's StoreVectors uses it ONCE per resident to materialize the vector run file as raw
 * doubles, and StoreSeed to seed the pool; the per-round cost/sample passes then read raw (no re-decode).</li>
 * <li>{@link PoolEnvelopeWriter} — emits pool members as the inter-stage {@code [kind, partition, seq, score,
 * vector]} open-list envelope (kind = 0 = pool) that the terminal WEIGH consumes unchanged. Op1 uses it, on
 * partition 0, to emit the final pool downstream.</li>
 * </ul>
 * This logic is duplicated (not shared) from {@code KMeansStageRuntime} to leave that committed
 * runtime untouched; the byte-compatibility is verified by the Route B == tower parity test. A later
 * cleanup may extract a single source of truth.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY Route B: input-vector decoder + KIND_POOL envelope writer (Op1 boundary codec)")
public final class KMeansVectorCodec {

    /** Envelope kind fields (match KMeansStageRuntime.KIND_*). */
    private static final double KIND_POOL = 0.0d;
    public static final double KIND_PARTIAL = 2.0d;

    private KMeansVectorCodec() {
    }

    /** Reusable decoder for an ordered-list-of-doubles vector column; one instance per operator task. */
    public static final class ListVectorDecoder {
        private final VoidPointable fieldPtr = new VoidPointable();
        private final ListAccessor listAccessor = new ListAccessor();
        private final VectorListDecoder decoder = new VectorListDecoder();

        public double[] decode(FrameTupleReference tuple, int col) throws HyracksDataException {
            try {
                fieldPtr.set(tuple.getFieldData(col), tuple.getFieldStart(col), tuple.getFieldLength(col));
                listAccessor.reset(fieldPtr.getByteArray(), fieldPtr.getStartOffset());
                double[] arr = new double[listAccessor.size()];
                return decoder.createArrayFromList(listAccessor, arr);
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    /**
     * Emits pool members as {@code KIND_POOL} open-list envelopes to a writer — the exact format the terminal
     * WEIGH's {@code collectPool} decodes. All items are tagged doubles; the vector is a nested open list.
     */
    public static final class PoolEnvelopeWriter {
        private final IFrameWriter writer;
        private final FrameTupleAppender appender;
        private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        private final OrderedListBuilder listBuilder = new OrderedListBuilder();
        private final OrderedListBuilder vecBuilder = new OrderedListBuilder();
        private final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
        private final ArrayBackedValueStorage vecStorage = new ArrayBackedValueStorage();
        private final AOrderedListType openList = new AOrderedListType(BuiltinType.ANY, null);

        public PoolEnvelopeWriter(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
            this.writer = writer;
            this.appender = new FrameTupleAppender(new VSizeFrame(ctx));
        }

        /** Appends one pool-member echo envelope {@code [KIND_POOL, 0, seq, 0.0, vec]} (partition 0). */
        public void poolMember(int seq, double[] vec) throws HyracksDataException {
            envelope(KIND_POOL, 0, seq, 0.0d, vec);
        }

        /**
         * Appends one general inter-stage envelope {@code [kind, partition, seq, score, vec]} — the exact format
         * WEIGH emits and RECLUSTER decodes. For a partial: {@code kind=KIND_PARTIAL}, {@code seq}=pool position,
         * {@code score}=count, {@code vec}=running sum.
         */
        public void envelope(double kind, int partition, int seq, double score, double[] vec)
                throws HyracksDataException {
            try {
                tb.reset();
                listBuilder.reset(openList);
                addDoubleItem(listBuilder, kind);
                addDoubleItem(listBuilder, partition);
                addDoubleItem(listBuilder, seq);
                addDoubleItem(listBuilder, score);
                vecBuilder.reset(openList);
                for (double d : vec) {
                    addDoubleItem(vecBuilder, d);
                }
                vecStorage.reset();
                vecBuilder.write(vecStorage.getDataOutput(), true);
                listBuilder.addItem(vecStorage);
                listBuilder.write(tb.getDataOutput(), true);
                tb.addFieldEndOffset();
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        public void flush() throws HyracksDataException {
            appender.write(writer, true);
        }

        private void addDoubleItem(OrderedListBuilder builder, double value) throws HyracksDataException {
            try {
                itemStorage.reset();
                itemStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                itemStorage.getDataOutput().writeDouble(value);
                builder.addItem(itemStorage);
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
