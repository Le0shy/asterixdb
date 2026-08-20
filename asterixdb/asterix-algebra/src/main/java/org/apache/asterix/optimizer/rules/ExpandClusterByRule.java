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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ClusterByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansStageOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
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
        Pair<Mutable<ILogicalOperator>, LogicalVariable> seedInput = copyOf(vectors, vectorVar, context);
        Mutable<ILogicalOperator> centroidsIn =
                seedOf(seedInput.first, seedInput.second, forgy ? cop.getNumClusters() : 1, context, loc);
        LogicalVariable centroidsVar = seedInput.second;

        if (!forgy) {
            Pair<Mutable<ILogicalOperator>, LogicalVariable> oversampleInput = copyOf(vectors, vectorVar, context);
            KMeansStageOperator oversample = stage(cop, KMeansStageOperator.Mode.OVERSAMPLE_LOOP, context,
                    ref(oversampleInput.second), ref(centroidsVar), oversamplingWidth(cop));
            oversample.setLoopRounds(OVERSAMPLING_ROUNDS);
            oversample.setSeed(SEED_BASE);
            oversample.getInputs().add(oversampleInput.first);
            oversample.getInputs().add(centroidsIn);
            oversample.recomputeSchema();
            context.computeAndSetTypeEnvironmentForOperator(oversample);

            KMeansStageOperator recluster = stage(cop, KMeansStageOperator.Mode.RECLUSTER, context, null,
                    ref(oversample.getCandidateVariable()), cop.getNumClusters());
            recluster.getInputs().add(new MutableObject<>(oversample));
            recluster.recomputeSchema();
            context.computeAndSetTypeEnvironmentForOperator(recluster);

            centroidsIn = new MutableObject<>(recluster);
            centroidsVar = recluster.getCandidateVariable();
        }

        KMeansStageOperator lloyd = stage(cop, KMeansStageOperator.Mode.LLOYD_LOOP, context, ref(vectorVar),
                ref(centroidsVar), cop.getNumClusters());
        lloyd.setLoopRounds(LLOYD_ITERATIONS);
        // The refinement stage returns the rows it held, each carrying its cluster and its distance to that
        // cluster's centre. These two are internal: the cluster id the query sees is the group-by's key, and
        // the distance is consumed by the radius aggregate.
        LogicalVariable rowCid = context.newVar();
        LogicalVariable rowDist = context.newVar();
        lloyd.setCandidateVariable(rowCid);
        lloyd.setDistanceVariable(rowDist, cop.getRadiusVarType());
        lloyd.getInputs().add(vectors);
        lloyd.getInputs().add(centroidsIn);
        lloyd.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(lloyd);
        return clustersOf(cop, lloyd, rowCid, rowDist, context, loc);
    }

    /**
     * Turns the labelled rows into one tuple per cluster.
     * <p>
     * This is an ordinary GROUP BY on the assignment, with the three things a cluster is made of hanging off
     * it as nested aggregates. It lives in the expansion rather than in the plan the query produced, because
     * from the optimizer's side CLUSTER BY is one operator -- how it is carried out is this rule's business.
     */
    private ILogicalOperator clustersOf(ClusterByOperator cop, ILogicalOperator labelled, LogicalVariable rowCid,
            LogicalVariable rowDist, IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        GroupByOperator gby = new GroupByOperator();
        gby.setSourceLocation(loc);
        gby.addGbyExpression(cop.getClusterIdVariable(), ref(rowCid).getValue());
        gby.getInputs().add(new MutableObject<>(labelled));

        // The members are the rows themselves, as the record the translator built; the centre is their mean,
        // which is where the reported centroid has always come from; the radius is the furthest member.
        gby.getNestedPlans().add(aggregate(gby, cop.getMembersVariable(), BuiltinFunctions.LISTIFY,
                cop.getMemberRecordRef().getValue(), context, loc));
        gby.getNestedPlans().add(aggregate(gby, cop.getCentroidVariable(), BuiltinFunctions.CENTROID,
                cop.getVectorRef().getValue(), context, loc));
        LogicalVariable maxDist = context.newVar();
        gby.getNestedPlans().add(aggregate(gby, maxDist, BuiltinFunctions.MAX, ref(rowDist).getValue(), context, loc));
        // Rules must set execution mode as well as schema: left unset the group-by stays unpartitioned and
        // property enforcement never repartitions on the key, so each partition groups only its own rows.
        OperatorManipulationUtil.setOperatorMode(gby);
        gby.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(gby);

        // A square root only undoes a squaring. Squared Euclidean returns d^2, so its radius is the root of
        // the max; cosine returns 1 - cos, an angle-like quantity whose root would mean nothing.
        ILogicalExpression radius = VectorSimilarityMetric.EUCLIDEAN_SQUARED.canonical().equals(cop.getMetric())
                ? new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.NUMERIC_SQRT), ref(maxDist))
                : ref(maxDist).getValue();
        AssignOperator radiusOp = new AssignOperator(cop.getRadiusVariable(), new MutableObject<>(radius));
        radiusOp.setSourceLocation(loc);
        radiusOp.getInputs().add(new MutableObject<>(gby));
        OperatorManipulationUtil.setOperatorMode(radiusOp);
        radiusOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(radiusOp);
        return radiusOp;
    }

    /** One nested aggregate over the group: {@code out <- fid(arg)}. */
    private ILogicalPlan aggregate(GroupByOperator gby, LogicalVariable out, FunctionIdentifier fid,
            ILogicalExpression arg, IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new MutableObject<>(gby));
        nts.setSourceLocation(loc);
        AggregateFunctionCallExpression call = BuiltinFunctions.makeAggregateFunctionExpression(fid,
                new ArrayList<>(List.of(new MutableObject<>(arg.cloneExpression()))));
        call.setSourceLocation(loc);
        AggregateOperator agg = new AggregateOperator(new ArrayList<>(List.of(out)),
                new ArrayList<>(List.of(new MutableObject<>(call))));
        agg.setSourceLocation(loc);
        agg.getInputs().add(new MutableObject<>(nts));
        // Rules that insert operators must set their schema themselves; nothing walks the plan afterwards
        // filling them in, and a nested tuple source without one fails at job generation.
        OperatorManipulationUtil.setOperatorMode(nts);
        nts.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(nts);
        OperatorManipulationUtil.setOperatorMode(agg);
        agg.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(agg);
        return new ALogicalPlanImpl(new MutableObject<>(agg));
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
        assign.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(assign);

        // Built as a top-n rather than a sort plus a limit. PushLimitIntoOrderByRule fuses those two in the
        // logical phase, which has already run by the time this rule fires, so an ordinary sort here would
        // stay a full sort of the whole input to take n rows from it.
        OrderOperator order = new OrderOperator(new ArrayList<>(), n);
        order.setSourceLocation(loc);
        order.getOrderExpressions().add(new Pair<>(OrderOperator.ASC_ORDER, ref(keyVar)));
        order.getInputs().add(new MutableObject<>(assign));
        order.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(order);

        // Both, deliberately: the top-n keeps the sort from ranking the whole input, and the limit is what
        // actually bounds the stream. That pair is the shape the plan had before this rule existed.
        LimitOperator limit = new LimitOperator(constant((long) n).getValue());
        limit.setSourceLocation(loc);
        limit.getInputs().add(new MutableObject<>(order));
        limit.recomputeSchema();
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
                new KMeansStageOperator(vectorRef, poolRef, context.newVar(), cop.getClusterIdVarType(), topCount);
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

    private static void recomputeSchemaBottomUp(ILogicalOperator op) throws AlgebricksException {
        for (Mutable<ILogicalOperator> child : op.getInputs()) {
            recomputeSchemaBottomUp(child.getValue());
        }
        if (op instanceof AbstractOperatorWithNestedPlans) {
            for (ILogicalPlan nested : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                for (Mutable<ILogicalOperator> root : nested.getRoots()) {
                    recomputeSchemaBottomUp(root.getValue());
                }
            }
        }
        op.recomputeSchema();
    }

    private static Mutable<ILogicalExpression> ref(LogicalVariable v) {
        return new MutableObject<>(new VariableReferenceExpression(v));
    }

    /**
     * An independent copy of the vector pipeline, with fresh variables.
     * <p>
     * The stages that read the vectors get a copy each rather than branches off a shared REPLICATE, because
     * emitting a straight-line plan and leaving sharing to be discovered is the order the framework is built
     * around: replicates are introduced in prepareForJobGenRewrites, where ExtractCommonOperatorsRule merges
     * equivalent subplans and names FixReplicateOperatorOutputsRule as its pre-condition. A replicate built
     * here instead, in physicalRewritesAllLevels, has its outputs list invalidated a few rules later when
     * EnforceStructuralPropertiesRule splices exchanges in, and nothing repairs it before job generation.
     * <p>
     * Fresh variables rather than shared ones: a stage reading two of its own inputs cannot tell which
     * stream a column came from if both carry the same variable.
     *
     * @return the copied root, and the variable its vector column is now called
     */
    private Pair<Mutable<ILogicalOperator>, LogicalVariable> copyOf(Mutable<ILogicalOperator> src,
            LogicalVariable vectorVar, IOptimizationContext context) throws AlgebricksException {
        Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> copy =
                OperatorManipulationUtil.deepCopyWithNewVars(src.getValue(), context);
        // deepCopyWithNewVars gives the copy type environments but not schemas, and an operator built on top
        // of it computes its own schema from its input's. Algebricks has no bottom-up schema pass, so the
        // copy gets one here.
        recomputeSchemaBottomUp(copy.first);
        LogicalVariable copiedVectorVar = copy.second.getOrDefault(vectorVar, vectorVar);
        return new Pair<>(new MutableObject<>(copy.first), copiedVectorVar);
    }
}
