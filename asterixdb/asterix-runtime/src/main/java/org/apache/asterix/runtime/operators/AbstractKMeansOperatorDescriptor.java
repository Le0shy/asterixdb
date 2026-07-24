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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Base for the CLUSTER BY k-means|| Score stages that consume a broadcast pool/partials stream. Two
 * activities; points never move: <b>StorePool</b> (the broadcast pool input, a sink) materializes its stream
 * as task state ({@link MaterializerTaskState}); <b>Score</b> is a SOURCE activity behind a blocking edge (an
 * input connector across a blocking-edge stage boundary is never delivered, so the pool must be materialized
 * rather than streamed in). The Score activity collects the pool via {@link KMeansStageRuntime} and delegates
 * the stage-specific work to {@link #emit}.
 * <p>
 * Subclasses fix the operator's input arity and the pool input's position ({@link #poolInputIndex}); a stage
 * that also consumes a second input (e.g. {@link KMeansWeighOperatorDescriptor}'s materialized vectors) adds
 * it via {@link #contributeInputActivities}.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY k-means|| Score stage base")
abstract class AbstractKMeansOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    protected static final int STORE_POOL_ACTIVITY_ID = 0;
    protected static final int SCORE_ACTIVITY_ID = 1;

    // WEIGH: intake re-limit; RECLUSTER/LLOYD: k. Non-negative.
    protected final int count;
    // Column of the pool variable in the pool input's tuples.
    protected final int poolColumn;
    // Whether the pool input carries envelopes from a prior tower stage (vs plain vectors, e.g. the centroids).
    protected final boolean poolIsEnvelope;

    protected AbstractKMeansOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor vectorRecDesc,
            int inputArity, int count, int poolColumn, boolean poolIsEnvelope) {
        super(spec, inputArity, 1);
        this.count = count;
        this.poolColumn = poolColumn;
        this.poolIsEnvelope = poolIsEnvelope;
        outRecDescs[0] = vectorRecDesc;
    }

    /** The operator input carrying the broadcast pool/partials (materialized by StorePool). */
    protected abstract int poolInputIndex();

    /**
     * Emits the stage result. The broadcast pool and partials are already collected into {@code rt}; a stage
     * that also reads a second (materialized) input reads its own task state from {@code ctx} here.
     */
    protected abstract void emit(KMeansStageRuntime rt, KMeansStageRuntime.Emitter emitter, IHyracksTaskContext ctx,
            int partition) throws Exception;

    /** Hook for a subclass to add extra input activities (e.g. WEIGH's vector store) before the Score edge. */
    protected void contributeInputActivities(IActivityGraphBuilder builder, IActivity score) {
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        StorePoolActivityNode storePool = new StorePoolActivityNode(new ActivityId(odId, STORE_POOL_ACTIVITY_ID));
        ScoreActivityNode score = new ScoreActivityNode(new ActivityId(odId, SCORE_ACTIVITY_ID));

        builder.addActivity(this, storePool);
        builder.addSourceEdge(poolInputIndex(), storePool, 0);

        builder.addActivity(this, score);
        builder.addTargetEdge(0, score, 0);

        builder.addBlockingEdge(storePool, score);
        contributeInputActivities(builder, score);
    }

    /** Materializes the broadcast pool input as task state for the Score activity to read. */
    private final class StorePoolActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private StorePoolActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private MaterializerTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
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
                @Override
                public void initialize() throws HyracksDataException {
                    writer.open();
                    try {
                        MaterializerTaskState poolState = (MaterializerTaskState) ctx.getStateObject(
                                new TaskId(new ActivityId(getOperatorId(), STORE_POOL_ACTIVITY_ID), partition));
                        KMeansStageRuntime rt =
                                new KMeansStageRuntime(ctx, writer, vecRecDesc, poolColumn, poolIsEnvelope, count);
                        rt.collectPool(poolState);
                        KMeansStageRuntime.Emitter emitter = rt.newEmitter();
                        emit(rt, emitter, ctx, partition);
                        emitter.flush();
                    } catch (Exception e) {
                        writer.fail();
                        throw HyracksDataException.create(e);
                    } finally {
                        writer.close();
                    }
                }
            };
        }
    }
}
