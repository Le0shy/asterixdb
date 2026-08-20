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
package org.apache.asterix.optimizer.rules;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ClusterByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansStageOperator;
import org.apache.hyracks.algebricks.rewriter.rules.AbstractDecorrelationRule;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Expands a {@link ClusterByOperator} into the chain of stages that implements its algorithm, the way the
 * combiner rules expand one group-by into a local and a global one.
 * <p>
 * Everything the query said is on the operator; everything about how the algorithm is carried out is decided
 * here. That split is the point: a second algorithm is another branch in {@link #expand}, not another path
 * through the language layer.
 * <p>
 * Runs at the head of the physical phase -- after every logical rule, so they all see one opaque node with one
 * ordinary input, and before physical-operator assignment and property enforcement, which need the stages.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Expansion of the algorithm-neutral CLUSTER BY node into its algorithm's stages")
public class ExpandClusterByRule extends AbstractDecorrelationRule {

    /** k-means||: rounds of oversampling, and the pool width drawn per round as a multiple of k. */
    private static final int OVERSAMPLING_ROUNDS = 5;
    private static final int OVERSAMPLING_FACTOR_PER_K = 2;
    /** Refinement iterations after seeding. */
    private static final int LLOYD_ITERATIONS = 3;
    /**
     * Base for the per-round sampling seed (seed_r = base + r); the descriptor mixes in the partition id.
     * The smallest prime above one million -- a prime stride keeps neighbouring (round, partition) pairs from
     * starting correlated generator states. Nothing depends on the magnitude.
     */
    private static final long SEED_BASE = 1_000_003L;

    private static final String ALGORITHM_KMEANS = "kmeans";
    private static final String INIT_MODE_RANDOM = "random";

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.CLUSTER_BY) {
            return false;
        }
        ClusterByOperator cop = (ClusterByOperator) op;
        if (!ALGORITHM_KMEANS.equals(cop.getAlgorithm())) {
            throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE,
                    "no expansion for CLUSTER BY algorithm " + cop.getAlgorithm());
        }
        opRef.setValue(expand(cop, context));
        return true;
    }

    /**
     * k-means||: oversample a pool from the seed, reduce it to k centres, then refine.
     * <p>
     * {@code random} skips the first two: its seed already <em>is</em> k centres, so refinement starts there.
     */
    private ILogicalOperator expand(ClusterByOperator cop, IOptimizationContext context) throws AlgebricksException {
        Mutable<ILogicalOperator> vectors = cop.getInputs().get(0);
        Mutable<ILogicalOperator> seed = cop.getInputs().get(1);
        LogicalVariable vectorVar = cop.getVectorVariable();
        LogicalVariable seedVar = cop.getPoolVariable();

        Mutable<ILogicalOperator> centroidsIn = seed;
        LogicalVariable centroidsVar = seedVar;
        if (!INIT_MODE_RANDOM.equals(cop.getInitMode())) {
            KMeansStageOperator oversample = stage(cop, KMeansStageOperator.Mode.OVERSAMPLE_LOOP, context,
                    ref(vectorVar), ref(seedVar), oversamplingWidth(cop));
            oversample.setLoopRounds(OVERSAMPLING_ROUNDS);
            oversample.setSeed(SEED_BASE);
            oversample.getInputs().add(copyOf(vectors, context));
            oversample.getInputs().add(seed);
            context.computeAndSetTypeEnvironmentForOperator(oversample);

            KMeansStageOperator recluster = stage(cop, KMeansStageOperator.Mode.RECLUSTER, context, null,
                    ref(oversample.getCandidateVariable()), cop.getNumClusters());
            recluster.getInputs().add(new MutableObject<>(oversample));
            context.computeAndSetTypeEnvironmentForOperator(recluster);

            centroidsIn = new MutableObject<>(recluster);
            centroidsVar = recluster.getCandidateVariable();
        }

        KMeansStageOperator lloyd = stage(cop, KMeansStageOperator.Mode.LLOYD_LOOP, context, ref(vectorVar),
                ref(centroidsVar), cop.getNumClusters());
        lloyd.setLoopRounds(LLOYD_ITERATIONS);
        // The expansion produces this operator's output, so it keeps the variable downstream already reads.
        lloyd.setCandidateVariable(cop.getCandidateVariable());
        lloyd.getInputs().add(vectors);
        lloyd.getInputs().add(centroidsIn);
        context.computeAndSetTypeEnvironmentForOperator(lloyd);
        return lloyd;
    }

    private KMeansStageOperator stage(ClusterByOperator cop, KMeansStageOperator.Mode mode,
            IOptimizationContext context, Mutable<ILogicalExpression> vectorRef, Mutable<ILogicalExpression> poolRef,
            int topCount) {
        KMeansStageOperator stage =
                new KMeansStageOperator(vectorRef, poolRef, context.newVar(), cop.getCandidateVarType(), topCount);
        stage.setMode(mode);
        stage.setMetric(cop.getMetric());
        stage.setSourceLocation(cop.getSourceLocation());
        return stage;
    }

    /** l = factor * k, the pool width per oversampling round. */
    private int oversamplingWidth(ClusterByOperator cop) throws AlgebricksException {
        try {
            return Math.multiplyExact(OVERSAMPLING_FACTOR_PER_K, cop.getNumClusters());
        } catch (ArithmeticException e) {
            throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, "CLUSTER BY num_clusters is too large: "
                    + OVERSAMPLING_FACTOR_PER_K + " * " + cop.getNumClusters() + " overflows a 32-bit integer");
        }
    }

    private static Mutable<ILogicalExpression> ref(LogicalVariable v) {
        return new MutableObject<>(new VariableReferenceExpression(v));
    }

    /**
     * The oversampling stage and the refinement stage each read the vectors. They are given separate copies
     * rather than a shared reference, because a plan is a tree here; ExtractCommonOperatorsRule merges them
     * behind a REPLICATE later, which is the same path the two copies took before this rule existed.
     */
    private Mutable<ILogicalOperator> copyOf(Mutable<ILogicalOperator> src, IOptimizationContext context)
            throws AlgebricksException {
        return new MutableObject<>(org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil
                .bottomUpCopyOperators(src.getValue()));
    }
}
