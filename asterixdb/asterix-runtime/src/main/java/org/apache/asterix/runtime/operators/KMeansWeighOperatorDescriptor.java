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
import java.util.List;

import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
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
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The CLUSTER BY k-means|| WEIGH stage: re-limit the intake, then stream the
 * local vectors ONCE against the decoded pool and accumulate per-pool-member (count, sum) partials; emits the
 * pool echo (partition 0) plus this partition's non-empty partials, for a downstream
 * {@link KMeansMergeOperatorDescriptor} to reduce.
 * <p>
 * Two inputs: input 0 is the partitioned vectors (materialized once per partition, {@link #STORE_VECTORS_ACTIVITY_ID});
 * input 1 is the broadcast pool ({@link AbstractKMeansOperatorDescriptor#poolInputIndex}). The vector stages of one
 * tower may share ONE materialized run file via {@code sharedVectorsKey}: exactly one stage is the writer (with
 * {@code sharedConsumerCount} readers; the file self-deletes after that many reads), the rest drain their vector
 * input without writing and read the writer's file.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "CLUSTER BY k-means|| weigh as a standalone two-input operator")
public final class KMeansWeighOperatorDescriptor extends AbstractKMeansOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int STORE_VECTORS_ACTIVITY_ID = 2;

    // Column of the vector variable in input 0's tuples.
    private final int vectorColumn;
    // Shared-vector materialization (optimization); null = this stage materializes its own vectors.
    private final String sharedVectorsKey;
    private final boolean vectorsWriter;
    private final int sharedConsumerCount;

    public KMeansWeighOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor vectorRecDesc, int count,
            int vectorColumn, int poolColumn, boolean poolIsEnvelope) {
        this(spec, vectorRecDesc, count, vectorColumn, poolColumn, poolIsEnvelope, null, false, 0);
    }

    public KMeansWeighOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor vectorRecDesc, int count,
            int vectorColumn, int poolColumn, boolean poolIsEnvelope, String sharedVectorsKey, boolean vectorsWriter,
            int sharedConsumerCount) {
        super(spec, vectorRecDesc, 2, count, poolColumn, poolIsEnvelope);
        this.vectorColumn = vectorColumn;
        this.sharedVectorsKey = sharedVectorsKey;
        this.vectorsWriter = vectorsWriter;
        this.sharedConsumerCount = sharedConsumerCount;
    }

    @Override
    protected int poolInputIndex() {
        return 1;
    }

    /** Joblet-scoped state id for this tower's shared vector run file on a given partition. */
    private Object sharedVectorsStateId(int partition) {
        return sharedVectorsKey + "#vec#" + partition;
    }

    @Override
    protected void contributeInputActivities(IActivityGraphBuilder builder, IActivity score) {
        StoreVectorsActivityNode storeVectors =
                new StoreVectorsActivityNode(new ActivityId(odId, STORE_VECTORS_ACTIVITY_ID));
        builder.addActivity(this, storeVectors);
        builder.addSourceEdge(0, storeVectors, 0);
        builder.addBlockingEdge(storeVectors, score);
    }

    @Override
    protected void emit(KMeansStageRuntime rt, KMeansStageRuntime.Emitter emitter, IHyracksTaskContext ctx,
            int partition) throws Exception {
        // Shared-vector stages read the writer's single run file by the shared key; otherwise this stage's own.
        Object vecId = sharedVectorsKey != null ? sharedVectorsStateId(partition)
                : new TaskId(new ActivityId(getOperatorId(), STORE_VECTORS_ACTIVITY_ID), partition);
        MaterializerTaskState vecState = (MaterializerTaskState) ctx.getStateObject(vecId);
        List<double[]> pool = rt.pool();
        final long[] counts = new long[pool.size()];
        final double[][] sums = new double[pool.size()][];
        rt.streamVectors(vecState, vectorColumn, vec -> {
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
                emitter.envelope(new KMeansStageRuntime.Row(KMeansStageRuntime.KIND_POOL, 0, i, 0.0d, pool.get(i)));
            }
        }
        for (int i = 0; i < pool.size(); i++) {
            if (counts[i] > 0) {
                emitter.envelope(
                        new KMeansStageRuntime.Row(KMeansStageRuntime.KIND_PARTIAL, partition, i, counts[i], sums[i]));
            }
        }
    }

    /**
     * Materializes input 0 (the partitioned vectors) as task state. When {@code sharedVectorsKey} is set, the
     * writer stage materializes ONE run file (self-deleting after {@code sharedConsumerCount} reads) and reader
     * stages drain their input without writing, reading the writer's file in {@link #emit}.
     */
    private final class StoreVectorsActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private StoreVectorsActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final boolean shared = sharedVectorsKey != null;
            // Shared-vectors READER stage: drain input 0 (satisfy the blocking edge) without materializing;
            // its emit reads the writer's shared run file instead.
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
}
