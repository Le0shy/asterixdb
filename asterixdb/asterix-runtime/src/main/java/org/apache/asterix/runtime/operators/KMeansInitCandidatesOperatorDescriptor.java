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
import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
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
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The CLUSTER BY k-means|| runtime stages as one Store+Store+Score operator, selected by {@link Mode}:
 * <ul>
 * <li><b>WEIGH</b> — re-limit the intake (the tower's terminal re-limit), then stream the local vectors
 * ONCE against the decoded pool and accumulate per-pool-member (count, sum) partials; emits the pool echo
 * (partition 0) plus this partition's partials.</li>
 * <li><b>RECLUSTER</b> — merge the (broadcast) partials deterministically and emit the {@code count}
 * heaviest members' means (weight DESC, pool position on ties) as plain vectors: the initial centroids
 * C0. Padded with pool members when fewer than {@code count} members attracted points.</li>
 * <li><b>LLOYD</b> — merge the (broadcast) partials and emit EVERY non-empty member's mean in pool order
 * (one Lloyd iteration's recomputed centroids) as plain vectors.</li>
 * </ul>
 * <p>
 * Three activities; points never move: <b>StoreVectors</b> (input 0, sink) and <b>StorePool</b>
 * (input 1, sink) materialize their streams as task state ({@link MaterializerTaskState}); <b>Score</b>
 * is a SOURCE activity behind two blocking edges (an input connector across a blocking-edge stage
 * boundary is never delivered, so both inputs must be materialized rather than streamed in).
 * <p>
 * <b>Chaining and the envelope format.</b> Tower stages chain linearly — a stage's pool input is the
 * previous stage's output — so between stages every row is an ENVELOPE: an open list
 * {@code [kind, partition, seq, score, vector]} (kind 0 = pool member, 1 = candidate, 2 = partial,
 * where a partial's seq is the pool position, score is the count, and vector is the running sum).
 * Partition 0 echoes the (broadcast, hence complete) pool through, so the output stream is directly the
 * next stage's pool.
 * <p>
 * <b>Global re-limit at intake.</b> The upstream OVERSAMPLE_LOOP emits up to P·l candidates (a LOCAL draw
 * per partition). WEIGH restores the algorithm's global l: the pool input is broadcast, so every partition
 * sees the identical row set and independently normalizes it — pool rows ordered by {@code seq}, plus the
 * GLOBAL top-{@code count} candidates ordered by (score DESC, partition ASC, seq ASC). All ordering derives
 * from envelope fields, never from frame arrival order (which differs per receiver), so all partitions
 * agree byte-for-byte.
 * <p>
 * Plain-vector output (RECLUSTER, LLOYD) is re-serialized as OPEN lists (tagged items): the output column
 * is typed ANY, and ANY-typed values must be self-describing — generic consumers (hash, comparators) walk
 * them by the compile-time type, so verbatim closed-format copies misparse.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY k-means|| init runtime, Store+Score stages")
public class KMeansInitCandidatesOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 2L;

    /** See the class comment. */
    public enum Mode {
        WEIGH,
        RECLUSTER,
        LLOYD
        // NB: the exact oversample loop (OVERSAMPLE_LOOP) is not a mode here — it is realized entirely as the
        // systolic 5-operator sub-graph (KMeans{CostController,PhiMerge,Sample,PoolMerge,Release}), injected by
        // KMeansInitCandidatesPOperator on every topology. The upstream loop produces the candidate pool this
        // descriptor's WEIGH then weighs.
    }

    private static final int STORE_VECTORS_ACTIVITY_ID = 0;
    private static final int STORE_POOL_ACTIVITY_ID = 1;
    private static final int SCORE_ACTIVITY_ID = 2;

    private static final double KIND_POOL = 0.0d;
    private static final double KIND_CANDIDATE = 1.0d;
    private static final double KIND_PARTIAL = 2.0d;

    private final Mode mode;
    // WEIGH: intake re-limit; RECLUSTER/LLOYD: k. Non-negative.
    private final int count;
    // Column of the vector variable in input 0's tuples and of the pool variable in input 1's tuples.
    private final int vectorColumn;
    private final int poolColumn;
    // Whether input 1 carries envelopes from a prior tower stage (vs plain vectors, e.g. the seed).
    private final boolean poolIsEnvelope;
    // Shared-vector materialization (optimization). When non-null, the WEIGH stages of one tower read ONE run
    // file registered (joblet-scoped) under this per-tower key + partition, instead of each stage
    // materializing its own copy. Exactly one stage is the writer: it materializes the run file with
    // sharedConsumerCount readers (the file self-deletes after that many reads); the other stages drain
    // their vector input without writing and read the writer's file. Null = original per-stage behavior
    // (also used by RECLUSTER/LLOYD, whose vector input is a LIMIT-1 dummy).
    private final String sharedVectorsKey;
    private final boolean vectorsWriter;
    private final int sharedConsumerCount;

    public KMeansInitCandidatesOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor vectorRecDesc,
            Mode mode, int count, int vectorColumn, int poolColumn, boolean poolIsEnvelope) {
        this(spec, vectorRecDesc, mode, count, vectorColumn, poolColumn, poolIsEnvelope, null, false, 0);
    }

    public KMeansInitCandidatesOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor vectorRecDesc,
            Mode mode, int count, int vectorColumn, int poolColumn, boolean poolIsEnvelope, String sharedVectorsKey,
            boolean vectorsWriter, int sharedConsumerCount) {
        super(spec, 2, 1);
        this.mode = mode;
        this.count = count;
        this.vectorColumn = vectorColumn;
        this.poolColumn = poolColumn;
        this.poolIsEnvelope = poolIsEnvelope;
        this.sharedVectorsKey = sharedVectorsKey;
        this.vectorsWriter = vectorsWriter;
        this.sharedConsumerCount = sharedConsumerCount;
        outRecDescs[0] = vectorRecDesc;
    }

    /** Joblet-scoped state id for this tower's shared vector run file on a given partition. */
    private Object sharedVectorsStateId(int partition) {
        return sharedVectorsKey + "#vec#" + partition;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        StoreActivityNode storeVectors = new StoreActivityNode(new ActivityId(odId, STORE_VECTORS_ACTIVITY_ID));
        StoreActivityNode storePool = new StoreActivityNode(new ActivityId(odId, STORE_POOL_ACTIVITY_ID));
        ScoreActivityNode score = new ScoreActivityNode(new ActivityId(odId, SCORE_ACTIVITY_ID));

        builder.addActivity(this, storeVectors);
        builder.addSourceEdge(0, storeVectors, 0);

        builder.addActivity(this, storePool);
        builder.addSourceEdge(1, storePool, 0);

        builder.addActivity(this, score);
        builder.addTargetEdge(0, score, 0);

        builder.addBlockingEdge(storeVectors, score);
        builder.addBlockingEdge(storePool, score);
    }

    private final class StoreActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private StoreActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            boolean vectorsSink = getActivityId().getLocalId() == STORE_VECTORS_ACTIVITY_ID;
            final boolean shared = vectorsSink && sharedVectorsKey != null;
            // Shared-vectors READER stage: drain input 0 (satisfy the blocking edge) without materializing;
            // its Score reads the writer's shared run file instead.
            if (shared && !vectorsWriter) {
                return new AbstractUnaryInputSinkOperatorNodePushable() {
                    @Override
                    public void open() throws HyracksDataException {
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    }

                    @Override
                    public void close() throws HyracksDataException {
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                    }
                };
            }
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private MaterializerTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    if (shared) {
                        // WRITER: one run file for the whole tower; self-deletes after sharedConsumerCount reads.
                        state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                                new TaskId(getActivityId(), partition), sharedConsumerCount);
                        state.setId(sharedVectorsStateId(partition));
                    } else {
                        state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                                new TaskId(getActivityId(), partition));
                    }
                    state.open(ctx);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.appendFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.close();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }

    /** One row of the chained stream: pool member, scored candidate, or (count, sum) partial. */
    private static final class Row {
        private final double kind;
        private final int partition;
        private final long seq;
        private final double score;
        private final double[] vec;

        private Row(double kind, int partition, long seq, double score, double[] vec) {
            this.kind = kind;
            this.partition = partition;
            this.seq = seq;
            this.score = score;
            this.vec = vec;
        }
    }

    /** Global candidate order: score DESC, then origin partition ASC, then arrival seq ASC. */
    private static final Comparator<Row> CANDIDATE_ORDER = (a, b) -> {
        int c = Double.compare(b.score, a.score);
        if (c != 0) {
            return c;
        }
        c = Integer.compare(a.partition, b.partition);
        return c != 0 ? c : Long.compare(a.seq, b.seq);
    };

    /** Deterministic partial merge order: pool position, then origin partition. */
    private static final Comparator<Row> PARTIAL_ORDER =
            Comparator.comparingLong((Row r) -> r.seq).thenComparingInt(r -> r.partition);

    private final class ScoreActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private ScoreActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final RecordDescriptor vecRecDesc = outRecDescs[0];
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                private final List<double[]> pool = new ArrayList<>();
                private final List<Row> partials = new ArrayList<>();
                private final FrameTupleReference tupleRef = new FrameTupleReference();
                private final VoidPointable fieldPtr = new VoidPointable();
                private final ListAccessor listAccessor = new ListAccessor();
                private final ListAccessor nestedAccessor = new ListAccessor();
                private final VectorListDecoder decoder = new VectorListDecoder();

                @Override
                public void initialize() throws HyracksDataException {
                    writer.open();
                    try {
                        MaterializerTaskState poolState = (MaterializerTaskState) ctx.getStateObject(
                                new TaskId(new ActivityId(getOperatorId(), STORE_POOL_ACTIVITY_ID), partition));
                        // Shared-vectors stages (ROUND/WEIGH) read the writer's single run file by the shared
                        // key; other stages read their own materialized vectors as before.
                        Object vecId = sharedVectorsKey != null ? sharedVectorsStateId(partition)
                                : new TaskId(new ActivityId(getOperatorId(), STORE_VECTORS_ACTIVITY_ID), partition);
                        MaterializerTaskState vecState = (MaterializerTaskState) ctx.getStateObject(vecId);
                        collectPool(poolState);
                        Emitter emitter = new Emitter();
                        switch (mode) {
                            case WEIGH:
                                emitWeigh(vecState, emitter);
                                break;
                            case RECLUSTER:
                                emitRecluster(emitter);
                                break;
                            case LLOYD:
                                emitLloyd(emitter);
                                break;
                        }
                        emitter.flush();
                    } catch (Exception e) {
                        writer.fail();
                        throw HyracksDataException.create(e);
                    } finally {
                        writer.close();
                    }
                }

                /**
                 * Builds this stage's pool from the materialized (broadcast) pool input. Plain-vector
                 * input is taken in arrival order; envelope input is NORMALIZED — pool rows by seq, plus
                 * the global top-{@code count} candidates in CANDIDATE_ORDER — identically on every
                 * partition, because the broadcast delivers the same row set everywhere and the order is
                 * derived from envelope fields only. Partial rows are kept aside for RECLUSTER.
                 */
                /**
                 * Cancellation responsiveness: Hyracks aborts tasks by interrupting their threads, but
                 * these loops are pure CPU over materialized run files — nothing blocks, so nothing
                 * throws. Poll the interrupt per frame (a few dozen tuples) in every pass.
                 */
                private void failIfInterrupted() throws HyracksDataException {
                    if (Thread.currentThread().isInterrupted()) {
                        throw HyracksDataException.create(new InterruptedException());
                    }
                }

                private void collectPool(MaterializerTaskState state) throws HyracksDataException {
                    final FrameTupleAccessor poolAccessor = new FrameTupleAccessor(vecRecDesc);
                    final List<Row> poolRows = new ArrayList<>();
                    final List<Row> candRows = new ArrayList<>();
                    state.writeOut(new org.apache.hyracks.api.comm.IFrameWriter() {
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
                                if (poolIsEnvelope) {
                                    Row r = decodeEnvelope(tupleRef, poolColumn);
                                    if (r.kind == KIND_POOL) {
                                        poolRows.add(r);
                                    } else if (r.kind == KIND_CANDIDATE) {
                                        candRows.add(r);
                                    } else {
                                        partials.add(r);
                                    }
                                } else {
                                    poolRows.add(new Row(KIND_POOL, 0, poolRows.size(), 0.0d,
                                            decodeVector(tupleRef, poolColumn)));
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

                private void emitWeigh(MaterializerTaskState state, Emitter emitter) throws Exception {
                    final long[] counts = new long[pool.size()];
                    final double[][] sums = new double[pool.size()][];
                    streamVectors(state, vec -> {
                        int bestIdx = -1;
                        double best = Double.POSITIVE_INFINITY;
                        for (int i = 0; i < pool.size(); i++) {
                            double d = VectorDistanceCalculation.euclideanSquared(vec, pool.get(i));
                            // Strict <: ties resolve to the first pool member, like nearest-centroid.
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
                    // Pool echo (partition 0), then this partition's non-empty partials.
                    if (partition == 0) {
                        for (int i = 0; i < pool.size(); i++) {
                            emitter.envelope(new Row(KIND_POOL, 0, i, 0.0d, pool.get(i)));
                        }
                    }
                    for (int i = 0; i < pool.size(); i++) {
                        if (counts[i] > 0) {
                            emitter.envelope(new Row(KIND_PARTIAL, partition, i, counts[i], sums[i]));
                        }
                    }
                }

                private void emitRecluster(Emitter emitter) throws Exception {
                    if (partition != 0) {
                        return; // the merged result is identical everywhere; one partition speaks
                    }
                    // Deterministic merge: partials ordered by (pool position, origin partition).
                    partials.sort(PARTIAL_ORDER);
                    long[] weights = new long[pool.size()];
                    double[][] sums = new double[pool.size()][];
                    for (Row p : partials) {
                        int idx = (int) p.seq;
                        if (idx < 0 || idx >= pool.size()) {
                            continue;
                        }
                        weights[idx] += (long) p.score;
                        double[] sum = sums[idx];
                        if (sum == null) {
                            sums[idx] = p.vec.clone();
                        } else {
                            for (int d = 0; d < Math.min(sum.length, p.vec.length); d++) {
                                sum[d] += p.vec[d];
                            }
                        }
                    }
                    // The count heaviest means (weight DESC, pool position ASC) ...
                    List<Integer> order = new ArrayList<>();
                    for (int i = 0; i < pool.size(); i++) {
                        if (weights[i] > 0) {
                            order.add(i);
                        }
                    }
                    order.sort((a, b) -> {
                        int c = Long.compare(weights[b], weights[a]);
                        return c != 0 ? c : Integer.compare(a, b);
                    });
                    int emitted = 0;
                    for (int i = 0; i < order.size() && emitted < count; i++, emitted++) {
                        int idx = order.get(i);
                        double[] mean = new double[sums[idx].length];
                        for (int d = 0; d < mean.length; d++) {
                            mean[d] = sums[idx][d] / weights[idx];
                        }
                        emitter.plainVector(mean);
                    }
                    // ... padded from the pool itself when fewer than count members attracted points
                    // (pool members are dataset points, mirroring the desugar's pad-from-the-data).
                    for (int i = 0; emitted < count && i < pool.size(); i++, emitted++) {
                        emitter.plainVector(pool.get(i));
                    }
                }

                /**
                 * One Lloyd iteration's central step: merge the (broadcast) partials deterministically
                 * and emit EVERY non-empty pool member's mean, in pool order. No ranking, no padding —
                 * a centroid that attracted no points is dropped, exactly like the reference GROUP BY.
                 */
                private void emitLloyd(Emitter emitter) throws Exception {
                    if (partition != 0) {
                        return; // the merged result is identical everywhere; one partition speaks
                    }
                    partials.sort(PARTIAL_ORDER);
                    long[] weights = new long[pool.size()];
                    double[][] sums = new double[pool.size()][];
                    for (Row p : partials) {
                        int idx = (int) p.seq;
                        if (idx < 0 || idx >= pool.size()) {
                            continue;
                        }
                        weights[idx] += (long) p.score;
                        double[] sum = sums[idx];
                        if (sum == null) {
                            sums[idx] = p.vec.clone();
                        } else {
                            for (int d = 0; d < Math.min(sum.length, p.vec.length); d++) {
                                sum[d] += p.vec[d];
                            }
                        }
                    }
                    for (int i = 0; i < pool.size(); i++) {
                        if (weights[i] > 0) {
                            double[] mean = new double[sums[i].length];
                            for (int d = 0; d < mean.length; d++) {
                                mean[d] = sums[i][d] / weights[i];
                            }
                            emitter.plainVector(mean);
                        }
                    }
                }

                private void streamVectors(MaterializerTaskState state, VectorSink sink) throws HyracksDataException {
                    final FrameTupleAccessor vecAccessor = new FrameTupleAccessor(vecRecDesc);
                    state.writeOut(new org.apache.hyracks.api.comm.IFrameWriter() {
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

                    private void envelope(Row row) throws Exception {
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
                        FrameUtilsHelper.appendToWriter(writer, appender, tb);
                    }

                    private void plainVector(double[] vec) throws Exception {
                        tb.reset();
                        buildVector(vec);
                        vecBuilder.write(tb.getDataOutput(), true);
                        tb.addFieldEndOffset();
                        FrameUtilsHelper.appendToWriter(writer, appender, tb);
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

                    private void flush() throws HyracksDataException {
                        appender.write(writer, true);
                    }
                }

            };
        }
    }

    @FunctionalInterface
    private interface VectorSink {
        void accept(double[] vec) throws HyracksDataException;
    }

    /** Tiny indirection so the appender flush idiom stays in one place. */
    private static final class FrameUtilsHelper {
        private static void appendToWriter(org.apache.hyracks.api.comm.IFrameWriter writer, FrameTupleAppender appender,
                ArrayTupleBuilder tb) throws HyracksDataException {
            org.apache.hyracks.dataflow.common.comm.util.FrameUtils.appendToWriter(writer, appender,
                    tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }
    }
}
