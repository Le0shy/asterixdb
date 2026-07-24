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
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorListDecoder;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct-drive tests of the Store+Score pair. Round mode: materialize a partition of 2-D vectors,
 * score them against a broadcast single-point pool, and assert the emitted ENVELOPE stream — the
 * echoed pool (partition 0) followed by the local top-l candidates in order. Finalize mode: feed an
 * envelope stream from two origin partitions and assert the deterministic global re-limit
 * (score DESC, origin partition ASC, seq ASC) and the plain-vector output.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "CLUSTER BY k-means|| init runtime, plumbing test")
public class KMeansInitCandidatesOperatorTest {

    @SuppressWarnings("rawtypes")
    private static final RecordDescriptor VEC_REC_DESC =
            new RecordDescriptor(new ISerializerDeserializer[] { ByteArraySerializerDeserializer.INSTANCE });

    @Test
    public void weighEmitsPoolEchoAndPartials() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(32768);
        JobSpecification spec = new JobSpecification();
        // WEIGH with intake limit 1: envelope pool (0,0) plus top-1 of the candidates -> pool [(0,0),(8,8)].
        KMeansInitCandidatesOperatorDescriptor op = new KMeansInitCandidatesOperatorDescriptor(spec, VEC_REC_DESC,
                KMeansInitCandidatesOperatorDescriptor.Mode.WEIGH, 1, 0, 0, true);

        List<IActivity> activities = collectActivities(op);
        IRecordDescriptorProvider rdp = recordDescProvider();

        IOperatorNodePushable store = activities.get(0).createPushRuntime(ctx, rdp, 0, 1);
        store.getInputFrameWriter(0).open();
        store.getInputFrameWriter(0).nextFrame(vectorsFrame(ctx, new double[][] { { 0, 0 }, { 1, 0 }, { 8, 9 } }));
        store.getInputFrameWriter(0).close();

        IOperatorNodePushable poolStore = activities.get(1).createPushRuntime(ctx, rdp, 0, 1);
        poolStore.getInputFrameWriter(0).open();
        poolStore.getInputFrameWriter(0)
                .nextFrame(envelopesFrame(ctx,
                        new double[][] {
                                // kind, partition, seq, score, v0, v1
                                { 0, 0, 0, 0, 0, 0 }, // pool member (0,0)
                                { 1, 0, 0, 128, 8, 8 }, // candidate (8,8) — survives the top-1 intake
                                { 1, 1, 0, 2, 1, 1 } })); // candidate (1,1) — cut
        poolStore.getInputFrameWriter(0).close();

        IOperatorNodePushable score = activities.get(2).createPushRuntime(ctx, rdp, 0, 1);
        List<double[]> out = collectOutput(score, true);

        // Echoed normalized pool, then this partition's partials: (0,0),(1,0) -> pool idx 0
        // (count 2, sum (1,0)); (8,9) -> pool idx 1 (count 1, sum (8,9)).
        Assert.assertEquals(4, out.size());
        Assert.assertArrayEquals(new double[] { 0, 0, 0, 0, 0, 0 }, out.get(0), 0.0);
        Assert.assertArrayEquals(new double[] { 0, 0, 1, 0, 8, 8 }, out.get(1), 0.0);
        Assert.assertArrayEquals(new double[] { 2, 0, 0, 2, 1, 0 }, out.get(2), 0.0);
        Assert.assertArrayEquals(new double[] { 2, 0, 1, 1, 8, 9 }, out.get(3), 0.0);
    }

    @Test
    public void reclusterMergesPartialsRanksAndPads() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(32768);
        JobSpecification spec = new JobSpecification();
        // RECLUSTER to k=3 means; only 2 pool members attracted points, so the third is a pool pad.
        KMeansInitCandidatesOperatorDescriptor op = new KMeansInitCandidatesOperatorDescriptor(spec, VEC_REC_DESC,
                KMeansInitCandidatesOperatorDescriptor.Mode.RECLUSTER, 3, 0, 0, true);

        List<IActivity> activities = collectActivities(op);
        IRecordDescriptorProvider rdp = recordDescProvider();

        // The vector input is unused in recluster mode; store a dummy row.
        IOperatorNodePushable store = activities.get(0).createPushRuntime(ctx, rdp, 0, 1);
        store.getInputFrameWriter(0).open();
        store.getInputFrameWriter(0).nextFrame(vectorsFrame(ctx, new double[][] { { 0, 0 } }));
        store.getInputFrameWriter(0).close();

        IOperatorNodePushable poolStore = activities.get(1).createPushRuntime(ctx, rdp, 0, 1);
        poolStore.getInputFrameWriter(0).open();
        poolStore.getInputFrameWriter(0)
                .nextFrame(envelopesFrame(ctx,
                        new double[][] {
                                // kind, partition, seq, score, v0, v1 — scrambled arrival on purpose
                                { 2, 1, 0, 1, 1, 2 }, // partial: idx 0, count 1, sum (1,2)
                                { 0, 0, 1, 0, 8, 8 }, // pool member 1 = (8,8)
                                { 2, 1, 1, 3, 24, 27 }, // partial: idx 1, count 3, sum (24,27)
                                { 0, 0, 0, 0, 0, 0 }, // pool member 0 = (0,0)
                                { 2, 0, 0, 2, 2, 4 } })); // partial: idx 0, count 2, sum (2,4)
        poolStore.getInputFrameWriter(0).close();

        IOperatorNodePushable score = activities.get(2).createPushRuntime(ctx, rdp, 0, 1);
        List<double[]> out = collectOutput(score, false);

        // idx 0: weight 3, mean (1,2); idx 1: weight 3, mean (8,9). Tie -> pool position ASC, so
        // idx 0 first; then the k=3 pad from pool position 0.
        Assert.assertEquals(3, out.size());
        Assert.assertArrayEquals(new double[] { 1, 2 }, out.get(0), 0.0);
        Assert.assertArrayEquals(new double[] { 8, 9 }, out.get(1), 0.0);
        Assert.assertArrayEquals(new double[] { 0, 0 }, out.get(2), 0.0);
    }

    @Test
    public void lloydMergeEmitsNonEmptyMeansInPoolOrder() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(32768);
        JobSpecification spec = new JobSpecification();
        // LLOYD with k=3: three pool members, the middle one attracts nothing -> exactly two means,
        // in pool order, no ranking, no pad.
        KMeansInitCandidatesOperatorDescriptor op = new KMeansInitCandidatesOperatorDescriptor(spec, VEC_REC_DESC,
                KMeansInitCandidatesOperatorDescriptor.Mode.LLOYD, 3, 0, 0, true);

        List<IActivity> activities = collectActivities(op);
        IRecordDescriptorProvider rdp = recordDescProvider();

        IOperatorNodePushable store = activities.get(0).createPushRuntime(ctx, rdp, 0, 1);
        store.getInputFrameWriter(0).open();
        store.getInputFrameWriter(0).nextFrame(vectorsFrame(ctx, new double[][] { { 0, 0 } }));
        store.getInputFrameWriter(0).close();

        IOperatorNodePushable poolStore = activities.get(1).createPushRuntime(ctx, rdp, 0, 1);
        poolStore.getInputFrameWriter(0).open();
        poolStore.getInputFrameWriter(0)
                .nextFrame(envelopesFrame(ctx,
                        new double[][] {
                                // kind, partition, seq, score, v0, v1
                                { 0, 0, 0, 0, 0, 0 }, // pool member 0
                                { 0, 0, 1, 0, 4, 4 }, // pool member 1 — attracts nothing, must be dropped
                                { 0, 0, 2, 0, 8, 8 }, // pool member 2
                                { 2, 0, 0, 2, 2, 4 }, // partial: idx 0, count 2, sum (2,4)
                                { 2, 1, 2, 1, 6, 6 } })); // partial: idx 2, count 1, sum (6,6)
        poolStore.getInputFrameWriter(0).close();

        IOperatorNodePushable score = activities.get(2).createPushRuntime(ctx, rdp, 0, 1);
        List<double[]> out = collectOutput(score, false);

        Assert.assertEquals(2, out.size());
        Assert.assertArrayEquals(new double[] { 1, 2 }, out.get(0), 0.0);
        Assert.assertArrayEquals(new double[] { 6, 6 }, out.get(1), 0.0);
    }

    private static List<IActivity> collectActivities(KMeansInitCandidatesOperatorDescriptor op) {
        List<IActivity> activities = new ArrayList<>();
        op.contributeActivities(new IActivityGraphBuilder() {
            @Override
            public void addActivity(IOperatorDescriptor o, IActivity task) {
                activities.add(task);
            }

            @Override
            public void addBlockingEdge(IActivity blocker, IActivity blocked) {
            }

            @Override
            public void addSourceEdge(int operatorInputIndex, IActivity task, int taskInputIndex) {
            }

            @Override
            public void addTargetEdge(int operatorOutputIndex, IActivity task, int taskOutputIndex) {
            }
        });
        return activities;
    }

    private static IRecordDescriptorProvider recordDescProvider() {
        return new IRecordDescriptorProvider() {
            @Override
            public RecordDescriptor getInputRecordDescriptor(ActivityId aid, int inputIndex) {
                return VEC_REC_DESC;
            }

            @Override
            public RecordDescriptor getOutputRecordDescriptor(ActivityId aid, int outputIndex) {
                return VEC_REC_DESC;
            }
        };
    }

    /** Runs the Score pushable and decodes every output row (envelope rows flattened when asked). */
    private static List<double[]> collectOutput(IOperatorNodePushable score, boolean envelopes) throws Exception {
        List<double[]> out = new ArrayList<>();
        score.setOutputFrameWriter(0, new IFrameWriter() {
            private final FrameTupleAccessor fta = new FrameTupleAccessor(VEC_REC_DESC);

            @Override
            public void open() {
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                fta.reset(buffer);
                for (int i = 0; i < fta.getTupleCount(); i++) {
                    FrameTupleReference ref = new FrameTupleReference();
                    ref.reset(fta, i);
                    out.add(envelopes ? decodeEnvelope(ref) : decodeVector(ref));
                }
            }

            @Override
            public void fail() {
            }

            @Override
            public void close() {
            }
        }, VEC_REC_DESC);
        score.initialize();
        return out;
    }

    private static double[] decodeVector(FrameTupleReference tuple) throws HyracksDataException {
        try {
            VoidPointable p = new VoidPointable();
            p.set(tuple.getFieldData(0), tuple.getFieldStart(0), tuple.getFieldLength(0));
            ListAccessor la = new ListAccessor();
            la.reset(p.getByteArray(), p.getStartOffset());
            return new VectorListDecoder().createArrayFromList(la, new double[la.size()]);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /** Flattens an envelope row to [kind, partition, seq, score, vector...]. */
    private static double[] decodeEnvelope(FrameTupleReference tuple) throws HyracksDataException {
        try {
            VoidPointable p = new VoidPointable();
            p.set(tuple.getFieldData(0), tuple.getFieldStart(0), tuple.getFieldLength(0));
            ListAccessor la = new ListAccessor();
            la.reset(p.getByteArray(), p.getStartOffset());
            byte[] bytes = la.getByteArray();
            ListAccessor vecAccessor = new ListAccessor();
            vecAccessor.reset(bytes, la.getItemOffset(4));
            double[] vec = new VectorListDecoder().createArrayFromList(vecAccessor, new double[vecAccessor.size()]);
            double[] flat = new double[4 + vec.length];
            for (int i = 0; i < 4; i++) {
                flat[i] = org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer
                        .getDouble(bytes, la.getItemOffset(i) + 1);
            }
            System.arraycopy(vec, 0, flat, 4, vec.length);
            return flat;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /** Builds a frame of envelope rows, each given as [kind, partition, seq, score, v...]. */
    private static ByteBuffer envelopesFrame(IHyracksTaskContext ctx, double[][] rows) throws HyracksDataException {
        try {
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender appender = new FrameTupleAppender(frame);
            ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
            OrderedListBuilder lb = new OrderedListBuilder();
            OrderedListBuilder vb = new OrderedListBuilder();
            ArrayBackedValueStorage item = new ArrayBackedValueStorage();
            ArrayBackedValueStorage vecStorage = new ArrayBackedValueStorage();
            AOrderedListType openList = new AOrderedListType(BuiltinType.ANY, null);
            for (double[] row : rows) {
                tb.reset();
                lb.reset(openList);
                for (int i = 0; i < 4; i++) {
                    item.reset();
                    item.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                    item.getDataOutput().writeDouble(row[i]);
                    lb.addItem(item);
                }
                vb.reset(openList);
                for (int i = 4; i < row.length; i++) {
                    item.reset();
                    item.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                    item.getDataOutput().writeDouble(row[i]);
                    vb.addItem(item);
                }
                vecStorage.reset();
                vb.write(vecStorage.getDataOutput(), true);
                lb.addItem(vecStorage);
                lb.write(tb.getDataOutput(), true);
                tb.addFieldEndOffset();
                Assert.assertTrue(appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize()));
            }
            return frame.getBuffer();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private static ByteBuffer vectorsFrame(IHyracksTaskContext ctx, double[][] vectors) throws HyracksDataException {
        try {
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender appender = new FrameTupleAppender(frame);
            ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
            OrderedListBuilder lb = new OrderedListBuilder();
            ArrayBackedValueStorage item = new ArrayBackedValueStorage();
            AOrderedListType listType = new AOrderedListType(BuiltinType.ADOUBLE, null);
            for (double[] vec : vectors) {
                tb.reset();
                lb.reset(listType);
                for (double d : vec) {
                    item.reset();
                    item.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                    item.getDataOutput().writeDouble(d);
                    lb.addItem(item);
                }
                lb.write(tb.getDataOutput(), true);
                tb.addFieldEndOffset();
                Assert.assertTrue(appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize()));
            }
            return frame.getBuffer();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    static {
        Arrays.hashCode(new int[0]); // keep Arrays import used across JDK format variations
    }
}
