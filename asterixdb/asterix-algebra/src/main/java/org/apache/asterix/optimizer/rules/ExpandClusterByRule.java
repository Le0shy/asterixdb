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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
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
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ClusterByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansStageOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
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
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
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
        // The rewrite rejects unknown algorithms; reaching this means a name was accepted without an expansion.
        throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, cop.getSourceLocation(),
                "no expansion for CLUSTER BY algorithm " + cop.getAlgorithm());
    }

    @FunctionalInterface
    private interface Expansion {
        ILogicalOperator expand(ClusterByOperator cop, IOptimizationContext context) throws AlgebricksException;
    }

    /**
     * k-means: seed, then refine. {@code kmeans_parallel} grows an oversampled pool from a single centre and
     * reduces it to k before refining; {@code random} (Forgy) takes k starting points and refines them directly.
     * The input is read once: one REPLICATE feeds the seed draw, the loops and the labelling.
     */
    private ILogicalOperator expandKMeans(ClusterByOperator cop, IOptimizationContext context)
            throws AlgebricksException {
        SourceLocation loc = cop.getSourceLocation();
        LogicalVariable vectorVar = cop.getVectorVariable();
        boolean forgy = INIT_MODE_RANDOM.equals(cop.getInitMode());
        rejectVectorsWiderThanAFrame(cop, context.getPhysicalOptimizationConfig().getFrameSize(), loc);

        // One REPLICATE over the input, as IntroduceSecondaryIndexInsertDeleteRule builds its fan-out: consumers
        // attached directly, outputs naming them, no exchanges -- the enforcer adds those, and
        // FixReplicateOperatorOutputsRule re-points the outputs before job generation. The seed draw and the
        // loops store their input before they run, so their outputs stream. The labelling join does not: its
        // probe side would run in the input's activity cluster while its build side, the final centroids,
        // depends on that same cluster -- a dependency cycle (ExtractCommonOperatorsRule.requiresMaterialization
        // is the same test). That last output is materialized, which gives the probe its own cluster.
        int outputArity = forgy ? 3 : 4;
        boolean[] materialize = new boolean[outputArity];
        materialize[outputArity - 1] = true;
        ReplicateOperator shared = new ReplicateOperator(outputArity, materialize);
        shared.setSourceLocation(loc);
        shared.getInputs().add(cop.getInputs().get(0));
        finish(shared, context);

        // Forgy seeds with k centres and refines them directly; k-means|| grows a pool from one.
        Pair<Mutable<ILogicalOperator>, LogicalVariable> seedInput = branchOf(shared, vectorVar, context, loc);
        Mutable<ILogicalOperator> centroidsIn = seedOf(seedInput.first, seedInput.second,
                forgy ? cop.getNumClusters() : 1, cop.getDimension(), context, loc);
        LogicalVariable centroidsVar = seedInput.second;
        if (!forgy) {
            KMeansStageOperator recluster =
                    oversampleAndRecluster(cop, shared, centroidsIn, centroidsVar, context, loc);
            centroidsIn = new MutableObject<>(recluster);
            centroidsVar = recluster.getCandidateVariable();
        }

        KMeansStageOperator lloyd = refine(cop, shared, centroidsIn, centroidsVar, context, loc);
        AggregateOperator finalSet = centroidList(lloyd, context, loc);
        LogicalVariable cFinal = finalSet.getVariables().get(0);

        Labelled rows = label(cop, shared, finalSet, cFinal, context, loc);
        return clustersOf(cop, rows.op, rows.cid, rows.radius, context, loc);
    }

    /**
     * A vector has to fit in a frame everywhere the stages sort, ship or store it. The sort would fail first,
     * with a message about sorting memory that points the user nowhere; the width is known here, so the
     * query is refused up front with the two numbers that matter.
     */
    private static void rejectVectorsWiderThanAFrame(ClusterByOperator cop, int frameSize, SourceLocation loc)
            throws AlgebricksException {
        // An open list of doubles: a tag and an 8-byte value per component, a 4-byte offset per component in
        // the list header, and the list's own header -- rounded up, so the check is never too lenient.
        long vectorBytes = 16L * cop.getDimension() + 64;
        if (vectorBytes > frameSize) {
            throw new CompilationException(org.apache.asterix.common.exceptions.ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'dimension' " + cop.getDimension() + " for " + cop.getClusteringExpression()
                            + " needs about " + vectorBytes + " bytes per vector, more than the frame size of "
                            + frameSize + " bytes; raise compiler.framesize or lower the dimension");
        }
    }

    /** k-means||: oversample a pool from the seed, then reduce it to k centres. */
    private KMeansStageOperator oversampleAndRecluster(ClusterByOperator cop, ReplicateOperator shared,
            Mutable<ILogicalOperator> seed, LogicalVariable seedVar, IOptimizationContext context, SourceLocation loc)
            throws AlgebricksException {
        Pair<Mutable<ILogicalOperator>, LogicalVariable> input =
                branchOf(shared, cop.getVectorVariable(), context, loc);
        KMeansStageOperator oversample = stage(cop, KMeansStageOperator.Mode.OVERSAMPLE_LOOP, context,
                ref(input.second), ref(seedVar), oversamplingWidth(cop));
        oversample.setLoopRounds(OVERSAMPLING_ROUNDS);
        oversample.setSeed(SEED_BASE);
        oversample.getInputs().add(input.first);
        oversample.getInputs().add(seed);
        finish(oversample, context);

        KMeansStageOperator recluster = stage(cop, KMeansStageOperator.Mode.RECLUSTER, context, null,
                ref(oversample.getCandidateVariable()), cop.getNumClusters());
        recluster.getInputs().add(new MutableObject<>(oversample));
        finish(recluster, context);
        return recluster;
    }

    /** The refinement loop: emits the k final centroids and nothing else. */
    private KMeansStageOperator refine(ClusterByOperator cop, ReplicateOperator shared,
            Mutable<ILogicalOperator> centroidsIn, LogicalVariable centroidsVar, IOptimizationContext context,
            SourceLocation loc) throws AlgebricksException {
        Pair<Mutable<ILogicalOperator>, LogicalVariable> input =
                branchOf(shared, cop.getVectorVariable(), context, loc);
        KMeansStageOperator lloyd = stage(cop, KMeansStageOperator.Mode.LLOYD_LOOP, context, ref(input.second),
                ref(centroidsVar), cop.getNumClusters());
        lloyd.setLoopRounds(LLOYD_ITERATIONS);
        lloyd.getInputs().add(input.first);
        lloyd.getInputs().add(centroidsIn);
        // The execution mode is derived from the input, as GROUP BY's is: a partitioned input gives one loop
        // instance per partition, an unpartitioned input a single instance. The physical operators read it
        // (AbstractKMeansStagePOperator.unpartitioned) to size and place the loop.
        finish(lloyd, context);
        return lloyd;
    }

    /**
     * The final centroid set as one list, ordered by centroid value: a cluster id is the position of the
     * nearest centroid in this list, and the loop's output order varies run to run. The aggregate is global, so
     * the enforcer sort-merges the partitions' streams into it.
     */
    private AggregateOperator centroidList(KMeansStageOperator lloyd, IOptimizationContext context, SourceLocation loc)
            throws AlgebricksException {
        LogicalVariable centroidVar = lloyd.getCandidateVariable();
        OrderOperator byValue = new OrderOperator();
        byValue.setSourceLocation(loc);
        byValue.getOrderExpressions().add(new Pair<>(OrderOperator.ASC_ORDER, ref(centroidVar)));
        byValue.getInputs().add(new MutableObject<>(lloyd));
        finish(byValue, context);

        AggregateFunctionCallExpression listify = BuiltinFunctions
                .makeAggregateFunctionExpression(BuiltinFunctions.LISTIFY, new ArrayList<>(List.of(ref(centroidVar))));
        listify.setSourceLocation(loc);
        AggregateOperator finalSet = new AggregateOperator(new ArrayList<>(List.of(context.newVar())),
                new ArrayList<>(List.of(new MutableObject<>(listify))));
        finalSet.setSourceLocation(loc);
        finalSet.setGlobal(true);
        finalSet.getInputs().add(new MutableObject<>(byValue));
        finish(finalSet, context);
        return finalSet;
    }

    /** The labelled rows: the operator at the top, the cluster-id variable, the radius variable or null. */
    private static final class Labelled {
        final ILogicalOperator op;
        final LogicalVariable cid;
        final LogicalVariable radius;

        Labelled(ILogicalOperator op, LogicalVariable cid, LogicalVariable radius) {
            this.op = op;
            this.cid = cid;
            this.radius = radius;
        }
    }

    /**
     * Labels every row. The last replicate branch carries the rows under their original variables -- the
     * vector and the member record. The single-tuple centroid list is attached to each row by a nested-loop
     * join on TRUE with the list side broadcast; then each row is labelled against it, and a row the labelling
     * cannot place (nearest-centroid returns NULL, with a warning, for a vector it cannot measure) is dropped
     * rather than grouped under a NULL key as a (k+1)-th cluster.
     */
    private Labelled label(ClusterByOperator cop, ReplicateOperator shared, AggregateOperator finalSet,
            LogicalVariable cFinal, IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        LogicalVariable vectorVar = cop.getVectorVariable();
        Mutable<ILogicalOperator> rows = new MutableObject<>(shared);
        shared.getOutputs().add(rows);
        InnerJoinOperator attach = new InnerJoinOperator(
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE))), rows,
                new MutableObject<>(finalSet));
        attach.setSourceLocation(loc);
        attach.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        finish(attach, context);

        Mutable<ILogicalExpression> metric =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AString(cop.getMetric()))));
        // The expression's text rides along so the evaluator can name it in what it reports about a row.
        Mutable<ILogicalExpression> named = new MutableObject<>(
                new ConstantExpression(new AsterixConstantValue(new AString(cop.getClusteringExpression()))));
        LogicalVariable rowCid = context.newVar();
        ScalarFunctionCallExpression nearest = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.NEAREST_CENTROID), ref(vectorVar), ref(cFinal),
                metric, named);
        nearest.setSourceLocation(loc);
        AssignOperator labelOp = new AssignOperator(rowCid, new MutableObject<>(nearest));
        labelOp.setSourceLocation(loc);
        labelOp.getInputs().add(new MutableObject<>(attach));
        finish(labelOp, context);

        // The per-row distance to the cluster's centre, feeding the radius aggregate. Built only when the query
        // reads cluster_radius: the unused-assign passes that follow this rule remove one layer each, which is
        // not enough to remove an unread distance together with its aggregate. The square root for squared
        // Euclidean is taken per row -- max of roots = root of max -- so the aggregate yields the radius itself.
        ILogicalOperator top = labelOp;
        LogicalVariable rowDist = null;
        if (cop.isRadiusRead()) {
            rowDist = context.newVar();
            ScalarFunctionCallExpression distance = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.NEAREST_CENTROID_DISTANCE), ref(vectorVar),
                    ref(cFinal), new MutableObject<>(metric.getValue().cloneExpression()),
                    new MutableObject<>(named.getValue().cloneExpression()));
            distance.setSourceLocation(loc);
            ILogicalExpression radiusOfRow = distance;
            if (VectorSimilarityMetric.EUCLIDEAN_SQUARED.canonical().equals(cop.getMetric())) {
                // Only a squaring is undone; cosine returns 1 - cos, whose root would mean nothing.
                ScalarFunctionCallExpression root = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.NUMERIC_SQRT),
                        new MutableObject<>(distance));
                root.setSourceLocation(loc);
                radiusOfRow = root;
            }
            AssignOperator measure = new AssignOperator(rowDist, new MutableObject<>(radiusOfRow));
            measure.setSourceLocation(loc);
            measure.getInputs().add(new MutableObject<>(labelOp));
            finish(measure, context);
            top = measure;
        }

        ScalarFunctionCallExpression unknown = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.IS_UNKNOWN), ref(rowCid));
        unknown.setSourceLocation(loc);
        ScalarFunctionCallExpression placed = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.NOT), new MutableObject<>(unknown));
        placed.setSourceLocation(loc);
        SelectOperator labelled = new SelectOperator(new MutableObject<>(placed));
        labelled.setSourceLocation(loc);
        labelled.getInputs().add(new MutableObject<>(top));
        finish(labelled, context);
        return new Labelled(labelled, rowCid, rowDist);
    }

    /** Sets what a rule-built operator must set itself: execution mode, schema, type environment. */
    private static void finish(AbstractLogicalOperator op, IOptimizationContext context) throws AlgebricksException {
        OperatorManipulationUtil.setOperatorMode(op);
        op.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(op);
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
        // The decorations ride on every labelled row; the GROUP BY carries them out as the operator promised.
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : cop.getDecorList()) {
            gby.addDecorExpression(p.first, p.second.getValue().cloneExpression());
        }
        gby.getInputs().add(new MutableObject<>(labelled));

        // members: the rows, as the record the translator built. centroid: their mean.
        gby.getNestedPlans().add(aggregate(gby, cop.getMembersVariable(), BuiltinFunctions.LISTIFY,
                cop.getMemberRecordRef().getValue(), context, loc));
        gby.getNestedPlans().add(aggregate(gby, cop.getCentroidVariable(), BuiltinFunctions.CENTROID,
                cop.getVectorRef().getValue(), context, loc));
        // cluster_radius: the furthest member's distance; the per-row value is already a radius.
        if (rowDist != null) {
            gby.getNestedPlans().add(aggregate(gby, cop.getRadiusVariable(), BuiltinFunctions.MAX,
                    ref(rowDist).getValue(), context, loc));
        }
        // Mode and schema are set explicitly: nothing walks the plan afterwards filling them in.
        finish(gby, context);
        return gby;
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
        // A nested tuple source without a schema fails at job generation.
        finish(nts, context);
        agg.getInputs().add(new MutableObject<>(nts));
        finish(agg, context);
        return new ALogicalPlanImpl(new MutableObject<>(agg));
    }

    /**
     * The {@code n} starting points, drawn uniformly from the vectors: order on a shuffle key and take the
     * first n.
     * <p>
     * The key is {@code random(vec[0])} rather than the vector itself. Ordering by VALUE would return the n
     * most similar points, seating every centre in one corner of the data, which is a stable fixed point that
     * no amount of refinement escapes.
     */
    private Mutable<ILogicalOperator> seedOf(Mutable<ILogicalOperator> vectors, LogicalVariable vectorVar, int n,
            int dimension, IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        // Only usable vectors may be drawn: a rejected draw loses the whole answer, and a row with no vector
        // makes random(v[0]) unknown, which orders first. The guard uses total functions the columnar filter
        // pushdown refuses (is-array, sql-count), so PushValueAccessAndFilterDownRule, which runs after this
        // rule, cannot push a conjunct into the scan, where it would be evaluated per array element.
        ScalarFunctionCallExpression isArray = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.IS_ARRAY), ref(vectorVar));
        isArray.setSourceLocation(loc);
        ScalarFunctionCallExpression width = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SCALAR_SQL_COUNT), ref(vectorVar));
        width.setSourceLocation(loc);
        ScalarFunctionCallExpression widthOk =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.EQ),
                        new MutableObject<>(width), constant((long) dimension));
        widthOk.setSourceLocation(loc);
        ScalarFunctionCallExpression usable = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND), new MutableObject<>(isArray),
                new MutableObject<>(widthOk));
        usable.setSourceLocation(loc);
        SelectOperator guard = new SelectOperator(new MutableObject<>(usable));
        guard.setSourceLocation(loc);
        guard.getInputs().add(vectors);
        finish(guard, context);

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
        assign.getInputs().add(new MutableObject<>(guard));
        finish(assign, context);

        // A top-n sort: PushLimitIntoOrderByRule, which would fuse a sort and a limit, has already run.
        OrderOperator order = new OrderOperator(new ArrayList<>(), n);
        order.setSourceLocation(loc);
        order.getOrderExpressions().add(new Pair<>(OrderOperator.ASC_ORDER, ref(keyVar)));
        order.getInputs().add(new MutableObject<>(assign));
        finish(order, context);

        // The top-n bounds the sort; the limit bounds the stream.
        LimitOperator limit = new LimitOperator(constant((long) n).getValue());
        limit.setSourceLocation(loc);
        limit.getInputs().add(new MutableObject<>(order));
        finish(limit, context);
        return new MutableObject<>(limit);
    }

    private static Mutable<ILogicalExpression> constant(long v) {
        return new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(v))));
    }

    private KMeansStageOperator stage(ClusterByOperator cop, KMeansStageOperator.Mode mode,
            IOptimizationContext context, Mutable<ILogicalExpression> vectorRef, Mutable<ILogicalExpression> poolRef,
            int topCount) {
        // Every stage emits vectors (a pool, the k candidates, the k centroids), typed open: their width is enforced
        // by the decoders, not by the type.
        KMeansStageOperator stage =
                new KMeansStageOperator(vectorRef, poolRef, context.newVar(), BuiltinType.ANY, topCount);
        stage.setMode(mode);
        stage.setMetric(cop.getMetric());
        stage.setClusteringExpression(cop.getClusteringExpression());
        // The loop stages admit only numeric arrays of this width; RECLUSTER reads decoded envelopes.
        stage.setDimension(cop.getDimension());
        stage.setSourceLocation(cop.getSourceLocation());
        return stage;
    }

    /** l = factor * k, the pool width per oversampling round. */
    private int oversamplingWidth(ClusterByOperator cop) throws AlgebricksException {
        try {
            return Math.multiplyExact(OVERSAMPLING_FACTOR_PER_K, cop.getNumClusters());
        } catch (ArithmeticException e) {
            throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, cop.getSourceLocation(),
                    "CLUSTER BY num_clusters is too large: " + OVERSAMPLING_FACTOR_PER_K + " * " + cop.getNumClusters()
                            + " overflows a 32-bit integer");
        }
    }

    private static Mutable<ILogicalExpression> ref(LogicalVariable v) {
        return new MutableObject<>(new VariableReferenceExpression(v));
    }

    /**
     * One consumer's branch off the shared input: an ASSIGN giving the vector a variable of the branch's own,
     * so no stage sees the same variable on two of its inputs (the seed stream feeds the oversample loop, whose
     * other input is the vectors).
     */
    private Pair<Mutable<ILogicalOperator>, LogicalVariable> branchOf(ReplicateOperator shared,
            LogicalVariable vectorVar, IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        LogicalVariable branchVar = context.newVar();
        AssignOperator rename = new AssignOperator(branchVar, ref(vectorVar));
        rename.setSourceLocation(loc);
        Mutable<ILogicalOperator> fromShared = new MutableObject<>(shared);
        shared.getOutputs().add(fromShared);
        rename.getInputs().add(fromShared);
        finish(rename, context);
        return new Pair<>(new MutableObject<>(rename), branchVar);
    }
}
