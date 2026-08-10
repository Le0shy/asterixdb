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
import java.util.Random;

import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
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
 * The CLUSTER BY k-means|| RECLUSTER stage -- a single-input Score operator that consumes ONLY the broadcast
 * partials and emits plain centroid vectors. It merges the partials deterministically, then reduces the
 * weighted candidate pool to the initial centroids C0 with weighted k-means++ (see
 * {@link #weightedKMeansPlusPlus}), which weighs each candidate's mass against its distance from the centroids
 * already chosen. The result is padded with pool members when fewer than {@code count} members attracted
 * points.
 * <p>
 * There is no vector input: the sole input is the broadcast partials envelope stream, so the stage is a pure
 * reduction over the partials. Two activities, and points never move between them. <b>StorePool</b> is a sink
 * that materializes the broadcast input as task state ({@link MaterializerTaskState}); <b>Score</b> is a SOURCE
 * activity behind a blocking edge, because an input connector across a blocking-edge stage boundary is never
 * delivered -- which is why the pool has to be materialized rather than streamed in. Score collects the pool
 * through {@link KMeansStageRuntime} and reduces it.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
public final class KMeansMergeOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int STORE_POOL_ACTIVITY_ID = 0;
    private static final int SCORE_ACTIVITY_ID = 1;

    // Seed for RECLUSTER's weighted k-means++ draw. The selection is randomized by nature, but CLUSTER BY
    // promises that the same query over the same data returns the same clusters, so the draw must not vary
    // between runs. A constant seed gives that: RECLUSTER runs on a single partition over one already-merged
    // pool, so one generator sequence covers the whole decision. The value itself carries no meaning, with one
    // constraint -- Random's constructor XORs the seed against its own multiplier (0x5DEECE66D), so that value
    // would zero the initial state and make the first draw 0.0, which turns the first centre into a fixed
    // choice of the lowest-indexed candidate rather than a weighted one.
    private static final long RECLUSTER_SEED = 12345L;

    // How many centroids to keep. Non-negative.
    private final int count;
    // Column of the pool variable in the input's tuples.
    private final int poolColumn;

    public KMeansMergeOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor vectorRecDesc, int count,
            int poolColumn) {
        // One input: the broadcast partials, which are always envelope rows (the oversampling loop's output).
        super(spec, 1, 1);
        this.count = count;
        this.poolColumn = poolColumn;
        outRecDescs[0] = vectorRecDesc;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        StorePoolActivityNode storePool = new StorePoolActivityNode(new ActivityId(odId, STORE_POOL_ACTIVITY_ID));
        ScoreActivityNode score = new ScoreActivityNode(new ActivityId(odId, SCORE_ACTIVITY_ID));

        builder.addActivity(this, storePool);
        builder.addSourceEdge(0, storePool, 0);

        builder.addActivity(this, score);
        builder.addTargetEdge(0, score, 0);

        builder.addBlockingEdge(storePool, score);
    }

    private void emitRecluster(KMeansStageRuntime rt, KMeansStageRuntime.Emitter emitter, int partition)
            throws Exception {
        if (partition != 0) {
            return; // the merged result is identical everywhere; one partition speaks
        }
        List<double[]> pool = rt.pool();
        List<KMeansStageRuntime.Row> partials = rt.partials();
        // Deterministic merge: partials ordered by (pool position, origin partition).
        partials.sort(KMeansStageRuntime.PARTIAL_ORDER);
        long[] weights = new long[pool.size()];
        double[][] sums = new double[pool.size()][];
        for (KMeansStageRuntime.Row p : partials) {
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
        // The weighted mean of every member that attracted points; these are what the reduction chooses from.
        List<double[]> means = new ArrayList<>();
        List<Long> memberWeights = new ArrayList<>();
        for (int i = 0; i < pool.size(); i++) {
            if (weights[i] > 0) {
                double[] mean = new double[sums[i].length];
                for (int d = 0; d < mean.length; d++) {
                    mean[d] = sums[i][d] / weights[i];
                }
                means.add(mean);
                memberWeights.add(weights[i]);
            }
        }
        int emitted = 0;
        for (double[] centroid : weightedKMeansPlusPlus(means, memberWeights)) {
            emitter.plainVector(centroid);
            emitted++;
        }
        // Fewer than count members can attract points (a pool member that nothing is closest to has no mean),
        // in which case top up from the pool itself. Pool members are dataset points, so the padding is drawn
        // from the data exactly as the equivalent SQL++ formulation pads.
        for (int i = 0; emitted < count && i < pool.size(); i++, emitted++) {
            emitter.plainVector(pool.get(i));
        }
    }

    /**
     * Reduces the weighted pool to at most {@code count} centroids with weighted k-means++ -- Bahmani et al.
     * (VLDB'12) Algorithm 2's closing step, "recluster the weighted points in C into k clusters". The first
     * centre is drawn with probability proportional to its weight; each further centre with probability
     * proportional to {@code w_x * d^2(x, chosen)}, so mass and distance both count.
     * <p>
     * Distance has to enter the choice here, because weight alone cannot separate the candidates. Oversampling
     * spreads its candidates evenly over the data, so each region's points divide among that region's own
     * candidates and every weight ends up in the same narrow band. Ranking by weight and taking the heaviest
     * {@code count} would therefore be close to an arbitrary pick: it can seat several centroids in one region
     * and leave another with none, and Lloyd cannot repair that afterwards because it only ever refines a
     * centroid within its own neighbourhood.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private List<double[]> weightedKMeansPlusPlus(List<double[]> means, List<Long> memberWeights) {
        List<double[]> chosen = new ArrayList<>();
        final int n = means.size();
        if (n == 0) {
            return chosen;
        }
        final double[] nearest = new double[n]; // d^2 to the closest already-chosen centre
        Arrays.fill(nearest, Double.POSITIVE_INFINITY);
        final boolean[] taken = new boolean[n];
        final Random rng = new Random(RECLUSTER_SEED);
        final double[] score = new double[n];
        final int target = Math.min(count, n);
        while (chosen.size() < target) {
            boolean first = chosen.isEmpty();
            double total = 0.0;
            for (int i = 0; i < n; i++) {
                // Before the first pick there is nothing to measure against, so weight alone drives the draw.
                double s = taken[i] ? 0.0 : memberWeights.get(i) * (first ? 1.0 : nearest[i]);
                score[i] = s > 0 && !Double.isNaN(s) && !Double.isInfinite(s) ? s : 0.0;
                total += score[i];
            }
            int pick = -1;
            if (total > 0) {
                double r = rng.nextDouble() * total;
                double acc = 0.0;
                for (int i = 0; i < n; i++) {
                    if (score[i] > 0) {
                        acc += score[i];
                        if (acc >= r) {
                            pick = i;
                            break;
                        }
                    }
                }
            }
            if (pick < 0) {
                // Every remaining member coincides with one already chosen (all weighted distances vanish).
                // Fall back to pool order so the outcome stays deterministic rather than dropping a centroid.
                for (int i = 0; i < n && pick < 0; i++) {
                    if (!taken[i]) {
                        pick = i;
                    }
                }
            }
            if (pick < 0) {
                break;
            }
            taken[pick] = true;
            chosen.add(means.get(pick));
            for (int i = 0; i < n; i++) {
                if (!taken[i]) {
                    double d = VectorDistanceCalculation.euclideanSquared(means.get(i), means.get(pick));
                    if (d < nearest[i]) {
                        nearest[i] = d;
                    }
                }
            }
        }
        return chosen;
    }

    /** Materializes the broadcast input as task state for the Score activity to read. */
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
                        KMeansStageRuntime rt = new KMeansStageRuntime(ctx, writer, vecRecDesc, poolColumn, count);
                        rt.collectPool(poolState);
                        KMeansStageRuntime.Emitter emitter = rt.newEmitter();
                        emitRecluster(rt, emitter, partition);
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
