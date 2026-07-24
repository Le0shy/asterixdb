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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The CLUSTER BY k-means|| merge stages -- single-input Score operators that consume ONLY the broadcast
 * partials and emit plain centroid vectors, selected by {@link Mode}:
 * <ul>
 * <li><b>RECLUSTER</b> — merge the partials deterministically and emit the {@code count} heaviest members'
 * means (weight DESC, pool position on ties) as the initial centroids C0. Padded with pool members when
 * fewer than {@code count} members attracted points.</li>
 * <li><b>LLOYD</b> — merge the partials and emit EVERY non-empty member's mean in pool order (one Lloyd
 * iteration's recomputed centroids); a centroid that attracted nothing is dropped, as GROUP BY would.</li>
 * </ul>
 * <p>
 * Both read a single input (the broadcast partials envelope stream, always {@code poolIsEnvelope}); there is
 * no vector input -- the merge is a pure reduction over the partials. See {@link AbstractKMeansOperatorDescriptor}.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "CLUSTER BY k-means|| recluster/lloyd merge as a single-input operator")
public final class KMeansMergeOperatorDescriptor extends AbstractKMeansOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public enum Mode {
        RECLUSTER,
        LLOYD
    }

    private final Mode mode;

    public KMeansMergeOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor vectorRecDesc, Mode mode,
            int count, int poolColumn) {
        // Single input: the broadcast partials, always envelope rows (a prior WEIGH / oversample-loop output).
        super(spec, vectorRecDesc, 1, count, poolColumn, true);
        this.mode = mode;
    }

    @Override
    protected int poolInputIndex() {
        return 0;
    }

    @Override
    protected void emit(KMeansStageRuntime rt, KMeansStageRuntime.Emitter emitter, IHyracksTaskContext ctx,
            int partition) throws Exception {
        switch (mode) {
            case RECLUSTER:
                emitRecluster(rt, emitter, partition);
                break;
            case LLOYD:
                emitLloyd(rt, emitter, partition);
                break;
        }
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
     * One Lloyd iteration's central step: merge the (broadcast) partials deterministically and emit EVERY
     * non-empty pool member's mean, in pool order. No ranking, no padding -- a centroid that attracted no
     * points is dropped, exactly like the reference GROUP BY.
     */
    private void emitLloyd(KMeansStageRuntime rt, KMeansStageRuntime.Emitter emitter, int partition) throws Exception {
        if (partition != 0) {
            return; // the merged result is identical everywhere; one partition speaks
        }
        List<double[]> pool = rt.pool();
        List<KMeansStageRuntime.Row> partials = rt.partials();
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
}
