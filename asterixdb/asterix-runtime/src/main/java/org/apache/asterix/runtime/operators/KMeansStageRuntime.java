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
package org.apache.asterix.runtime.operators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorListDecoder;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Per-task runtime shared by the CLUSTER BY k-means|| Score stages ({@link AbstractKMeansOperatorDescriptor}
 * and its subclasses): decodes the materialized (broadcast) pool/partials input into a pool + partial list,
 * optionally streams the materialized vector input, and serializes results. One instance per Score task.
 * <p>
 * The chained inter-stage row is an ENVELOPE: an open list {@code [kind, partition, seq, score, vector]}
 * (kind 0 = pool member, 1 = candidate, 2 = partial, where a partial's seq is the pool position, score is
 * the count, and vector is the running sum). Envelope input is NORMALIZED identically on every partition --
 * pool rows by seq, plus the global top-{@code count} candidates in {@link #CANDIDATE_ORDER} -- because the
 * broadcast delivers the same row set everywhere and the order is derived from envelope fields only.
 * Plain-vector output is re-serialized as OPEN lists (tagged items) since the output column is typed ANY.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
final class KMeansStageRuntime {

    static final double KIND_POOL = 0.0d;
    static final double KIND_CANDIDATE = 1.0d;
    static final double KIND_PARTIAL = 2.0d;

    /** One row of the chained stream: pool member, scored candidate, or (count, sum) partial. */
    static final class Row {
        final double kind;
        final int partition;
        final long seq;
        final double score;
        final double[] vec;

        Row(double kind, int partition, long seq, double score, double[] vec) {
            this.kind = kind;
            this.partition = partition;
            this.seq = seq;
            this.score = score;
            this.vec = vec;
        }
    }

    /** Global candidate order: score DESC, then origin partition ASC, then arrival seq ASC. */
    static final Comparator<Row> CANDIDATE_ORDER = (a, b) -> {
        int c = Double.compare(b.score, a.score);
        if (c != 0) {
            return c;
        }
        c = Integer.compare(a.partition, b.partition);
        return c != 0 ? c : Long.compare(a.seq, b.seq);
    };

    /** Deterministic partial merge order: pool position, then origin partition. */
    static final Comparator<Row> PARTIAL_ORDER =
            Comparator.comparingLong((Row r) -> r.seq).thenComparingInt(r -> r.partition);

    private final IHyracksTaskContext ctx;
    private final IFrameWriter writer;
    private final RecordDescriptor vecRecDesc;
    private final int poolColumn;
    private final int count;

    private final FrameTupleReference tupleRef = new FrameTupleReference();
    private final VoidPointable fieldPtr = new VoidPointable();
    private final ListAccessor listAccessor = new ListAccessor();
    private final ListAccessor nestedAccessor = new ListAccessor();
    private final VectorListDecoder decoder = new VectorListDecoder();

    private final List<double[]> pool = new ArrayList<>();
    private final List<Row> partials = new ArrayList<>();

    KMeansStageRuntime(IHyracksTaskContext ctx, IFrameWriter writer, RecordDescriptor vecRecDesc, int poolColumn,
            int count) {
        this.ctx = ctx;
        this.writer = writer;
        this.vecRecDesc = vecRecDesc;
        this.poolColumn = poolColumn;
        this.count = count;
    }

    List<double[]> pool() {
        return pool;
    }

    List<Row> partials() {
        return partials;
    }

    /**
     * Cancellation responsiveness: Hyracks aborts tasks by interrupting their threads, but these loops are
     * pure CPU over materialized run files -- nothing blocks, so nothing throws. Poll the interrupt per frame
     * (a few dozen tuples) in every pass.
     */
    private void failIfInterrupted() throws HyracksDataException {
        if (Thread.currentThread().isInterrupted()) {
            throw HyracksDataException.create(new InterruptedException());
        }
    }

    /**
     * Builds this stage's pool from the materialized (broadcast) pool input. Plain-vector input is taken in
     * arrival order; envelope input is normalized -- pool rows by seq, plus the global top-{@code count}
     * candidates in CANDIDATE_ORDER -- identically on every partition. Partial rows are kept aside for the
     * merge stages.
     */
    void collectPool(MaterializerTaskState state) throws HyracksDataException {
        final FrameTupleAccessor poolAccessor = new FrameTupleAccessor(vecRecDesc);
        final List<Row> poolRows = new ArrayList<>();
        final List<Row> candRows = new ArrayList<>();
        state.writeOut(new IFrameWriter() {
            @Override
            public void open() {
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                failIfInterrupted();
                poolAccessor.reset(buffer);
                int tupleCount = poolAccessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tupleRef.reset(poolAccessor, i);
                    Row r = decodeEnvelope(tupleRef, poolColumn);
                    if (r.kind == KIND_POOL) {
                        poolRows.add(r);
                    } else if (r.kind == KIND_CANDIDATE) {
                        candRows.add(r);
                    } else {
                        partials.add(r);
                    }
                }
            }

            @Override
            public void fail() {
            }

            @Override
            public void close() {
            }
        }, new VSizeFrame(ctx), false);
        poolRows.sort(Comparator.comparingLong(r -> r.seq));
        candRows.sort(CANDIDATE_ORDER);
        for (Row r : poolRows) {
            pool.add(r.vec);
        }
        int candLimit = Math.min(count, candRows.size());
        for (int i = 0; i < candLimit; i++) {
            pool.add(candRows.get(i).vec);
        }
    }

    /** Streams the materialized vector input, decoding each tuple's vector column and feeding it to sink. */
    void streamVectors(MaterializerTaskState state, int vectorColumn, VectorSink sink) throws HyracksDataException {
        final FrameTupleAccessor vecAccessor = new FrameTupleAccessor(vecRecDesc);
        state.writeOut(new IFrameWriter() {
            @Override
            public void open() {
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                failIfInterrupted();
                vecAccessor.reset(buffer);
                int tupleCount = vecAccessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tupleRef.reset(vecAccessor, i);
                    sink.accept(decodeVector(tupleRef, vectorColumn));
                }
            }

            @Override
            public void fail() {
            }

            @Override
            public void close() {
            }
        }, new VSizeFrame(ctx), false);
    }

    private double[] decodeVector(FrameTupleReference tuple, int col) throws HyracksDataException {
        try {
            fieldPtr.set(tuple.getFieldData(col), tuple.getFieldStart(col), tuple.getFieldLength(col));
            listAccessor.reset(fieldPtr.getByteArray(), fieldPtr.getStartOffset());
            double[] arr = new double[listAccessor.size()];
            return decoder.createArrayFromList(listAccessor, arr);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private Row decodeEnvelope(FrameTupleReference tuple, int col) throws HyracksDataException {
        try {
            fieldPtr.set(tuple.getFieldData(col), tuple.getFieldStart(col), tuple.getFieldLength(col));
            listAccessor.reset(fieldPtr.getByteArray(), fieldPtr.getStartOffset());
            byte[] bytes = listAccessor.getByteArray();
            double kind = envelopeDouble(bytes, 0);
            int origin = (int) envelopeDouble(bytes, 1);
            long seq = (long) envelopeDouble(bytes, 2);
            double score = envelopeDouble(bytes, 3);
            int vecOffset = listAccessor.getItemOffset(4);
            nestedAccessor.reset(bytes, vecOffset);
            double[] vec = decoder.createArrayFromList(nestedAccessor, new double[nestedAccessor.size()]);
            return new Row(kind, origin, seq, score, vec);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private double envelopeDouble(byte[] bytes, int itemIndex) throws HyracksDataException {
        // Envelope items are self-describing (open list): tag byte, then the payload.
        int offset = listAccessor.getItemOffset(itemIndex);
        return ADoubleSerializerDeserializer.getDouble(bytes, offset + 1);
    }

    Emitter newEmitter() throws HyracksDataException {
        return new Emitter();
    }

    @FunctionalInterface
    interface VectorSink {
        void accept(double[] vec) throws HyracksDataException;
    }

    /** Serialization state for one emit pass; every value is an OPEN list (tagged items). */
    final class Emitter {
        private final FrameTupleAppender appender;
        private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        private final OrderedListBuilder listBuilder = new OrderedListBuilder();
        private final OrderedListBuilder vecBuilder = new OrderedListBuilder();
        private final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
        private final ArrayBackedValueStorage vecStorage = new ArrayBackedValueStorage();
        private final AOrderedListType openList = new AOrderedListType(BuiltinType.ANY, null);

        private Emitter() throws HyracksDataException {
            appender = new FrameTupleAppender(new VSizeFrame(ctx));
        }

        void envelope(Row row) throws Exception {
            tb.reset();
            listBuilder.reset(openList);
            addDoubleItem(listBuilder, row.kind);
            addDoubleItem(listBuilder, row.partition);
            addDoubleItem(listBuilder, row.seq);
            addDoubleItem(listBuilder, row.score);
            buildVector(row.vec);
            vecStorage.reset();
            vecBuilder.write(vecStorage.getDataOutput(), true);
            listBuilder.addItem(vecStorage);
            listBuilder.write(tb.getDataOutput(), true);
            tb.addFieldEndOffset();
            appendToWriter();
        }

        void plainVector(double[] vec) throws Exception {
            tb.reset();
            buildVector(vec);
            vecBuilder.write(tb.getDataOutput(), true);
            tb.addFieldEndOffset();
            appendToWriter();
        }

        private void buildVector(double[] vec) throws Exception {
            vecBuilder.reset(openList);
            for (double d : vec) {
                addDoubleItem(vecBuilder, d);
            }
        }

        private void addDoubleItem(OrderedListBuilder builder, double value) throws Exception {
            itemStorage.reset();
            itemStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
            itemStorage.getDataOutput().writeDouble(value);
            builder.addItem(itemStorage);
        }

        private void appendToWriter() throws HyracksDataException {
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }

        void flush() throws HyracksDataException {
            appender.write(writer, true);
        }
    }
}
