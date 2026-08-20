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

import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ClusterByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansStageOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.rewriter.rules.AbstractDecorrelationRule;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Expands a {@link ClusterByOperator} into the chain of stages that implements its algorithm, the way the
 * combiner rules expand one group-by into a local and a global one.
 * <p>
 * Everything the query said is on the operator; everything about how the algorithm is carried out is decided
 * here. That split is the point: a second algorithm is another {@link Expansion}, not another path through
 * the language layer.
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
        opRef.setValue(expansionFor(cop).expand(cop, context));
        return true;
    }

    /**
     * The extension point. An algorithm is a method that turns this node into the stages implementing it;
     * adding one is adding a case here and accepting its name in the rewrite's option validation. Nothing
     * above this rule -- grammar, rewrite, translator, logical plan -- learns that the algorithm exists.
     */
    private Expansion expansionFor(ClusterByOperator cop) throws AlgebricksException {
        if (ALGORITHM_KMEANS.equals(cop.getAlgorithm())) {
            return this::expandKMeans;
        }
        // Unreachable: the rewrite rejects an unknown algorithm with a source location the user can act on.
        // Reached only if a name passes validation with no expansion behind it, which is our bug, not theirs.
        throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE,
                "no expansion for CLUSTER BY algorithm " + cop.getAlgorithm());
    }

    @FunctionalInterface
    private interface Expansion {
        ILogicalOperator expand(ClusterByOperator cop, IOptimizationContext context) throws AlgebricksException;
    }

    /**
     * k-means||: oversample a pool from the seed, reduce it to k centres, then refine.
     * <p>
     * {@code random} skips the first two: its seed already <em>is</em> k centres, so refinement starts there.
     */
    /**
     * k-means: seed, then refine. {@code kmeanspp} grows an oversampled pool from a single centre and reduces
     * it to k before refining; {@code random} (Forgy) takes k starting points as its answer and refines them
     * directly.
     */
    private ILogicalOperator expandKMeans(ClusterByOperator cop, IOptimizationContext context)
            throws AlgebricksException {
        SourceLocation loc = cop.getSourceLocation();
        Mutable<ILogicalOperator> vectors = cop.getInputs().get(0);
        LogicalVariable vectorVar = cop.getVectorVariable();
        boolean forgy = INIT_MODE_RANDOM.equals(cop.getInitMode());

        // Forgy seeds with k centres and refines them directly; k-means|| grows a pool from one.
        Mutable<ILogicalOperator> centroidsIn =
                seedOf(copyOf(vectors), vectorVar, forgy ? cop.getNumClusters() : 1, context, loc);
        LogicalVariable centroidsVar = vectorVar;

        if (!forgy) {
            KMeansStageOperator oversample = stage(cop, KMeansStageOperator.Mode.OVERSAMPLE_LOOP, context,
                    ref(vectorVar), ref(centroidsVar), oversamplingWidth(cop));
            oversample.setLoopRounds(OVERSAMPLING_ROUNDS);
            oversample.setSeed(SEED_BASE);
            oversample.getInputs().add(copyOf(vectors));
            oversample.getInputs().add(centroidsIn);
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

    /**
     * The {@code n} starting points, drawn uniformly from the vectors: order on a shuffle key and take the
     * first n.
     * <p>
     * The key is {@code random(vec[0])} rather than the vector itself. Ordering by VALUE would return the n
     * most similar points, seating every centre in one corner of the data, which is a stable fixed point that
     * no amount of refinement escapes.
     * <p>
     * Built here rather than by the rewrite because how many starting points an algorithm wants is a fact
     * about the algorithm: k-means|| grows a pool from one, Forgy uses k as its answer outright.
     */
    private Mutable<ILogicalOperator> seedOf(Mutable<ILogicalOperator> vectors, LogicalVariable vectorVar, int n,
            IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        ScalarFunctionCallExpression firstComponent = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.GET_ITEM), ref(vectorVar), constant(0L));
        firstComponent.setSourceLocation(loc);
        ScalarFunctionCallExpression key = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.RANDOM_WITH_SEED),
                new MutableObject<>(firstComponent));
        key.setSourceLocation(loc);

        LogicalVariable keyVar = context.newVar();
        AssignOperator assign = new AssignOperator(keyVar, new MutableObject<>(key));
        assign.setSourceLocation(loc);
        assign.getInputs().add(vectors);
        context.computeAndSetTypeEnvironmentForOperator(assign);

        OrderOperator order = new OrderOperator();
        order.setSourceLocation(loc);
        order.getOrderExpressions().add(new Pair<>(OrderOperator.ASC_ORDER, ref(keyVar)));
        order.getInputs().add(new MutableObject<>(assign));
        context.computeAndSetTypeEnvironmentForOperator(order);

        LimitOperator limit = new LimitOperator(constant((long) n).getValue());
        limit.setSourceLocation(loc);
        limit.getInputs().add(new MutableObject<>(order));
        context.computeAndSetTypeEnvironmentForOperator(limit);
        return new MutableObject<>(limit);
    }

    private static Mutable<ILogicalExpression> constant(long v) {
        return new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(v))));
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
    private Mutable<ILogicalOperator> copyOf(Mutable<ILogicalOperator> src) throws AlgebricksException {
        return new MutableObject<>(org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil
                .bottomUpCopyOperators(src.getValue()));
    }
}
