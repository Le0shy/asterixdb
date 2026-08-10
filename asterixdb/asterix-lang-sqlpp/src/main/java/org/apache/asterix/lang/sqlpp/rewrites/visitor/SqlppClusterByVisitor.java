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

package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.DoubleLiteral;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ConfigurationUtil;
import org.apache.asterix.lang.common.util.VectorDistanceMetric;
import org.apache.asterix.lang.sqlpp.clause.ClusterbyClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Rewrites {@code CLUSTER BY} into a distributed k-means query in plain SQL++. A block
 *
 * <pre>
 *   FROM src AS t
 *   CLUSTER BY t.vec AS sc [CLUSTER AS members]
 *   WITH { "NumClusters": k, ... }
 *   SELECT ... sc.cluster_id ... sc.centroid ... sc.cluster_radius ... members ...
 * </pre>
 *
 * becomes (conceptually)
 *
 * <pre>
 *   LET __vecs = (FROM src AS v SELECT VALUE v.vec),
 *       __pool = kmeans_oversample_loop(__vecs, __seed, 2k, 5, seedBase),       -- initMode "kmeansPP": exact
 *                                                                               --   k-means|| oversampling
 *                                                                               --   (VLDB'12), run at runtime
 *       C0 = kmeans_recluster(__vecs, __pool, k),                               -- weight the pool, reduce to k
 *       C1 = (FROM __vecs AS v SELECT VALUE centroid(v) GROUP BY nearest_centroid(v, C0)), -- Lloyd iter 1
 *       C2 = (... nearest_centroid(v, C1)),                                                -- iter 2
 *       C3 = (... nearest_centroid(v, C2))                                                 -- iter 3
 *   FROM src AS t
 *   GROUP BY nearest_centroid(t.vec, C3) AS $cid [GROUP AS members]
 *   SELECT ...   -- sc.cluster_id -&gt; nearest_centroid(t.vec, C3), sc.centroid -&gt; centroid(t.vec), radius -&gt; 0.0
 * </pre>
 *
 * For {@code initMode "random"} the {@code kmeans_oversample_loop}/{@code kmeans_recluster} init is skipped
 * entirely and Lloyd seeds directly from {@code k} vectors of {@code __vecs} drawn uniformly (Forgy); the
 * Lloyd stage still runs as the same runtime operator.
 *
 * The centroid lists {@code C0..C3} are query-level LETs (constants, in scope after the GROUP BY). The two-step
 * distributed CENTROID aggregate + {@code nearest_centroid} broadcast labeling are supplied by the downstream
 * group-by / aggregation rewrites, so this pass rides the normal SQL++ pipeline. It must run BEFORE
 * {@code substituteGroupbyKeyExpression()}/{@code rewriteGroupBys()} so the emitted GROUP BY is desugared like a
 * parsed one. The descriptor {@code sc} is not a materialized variable: its field accesses are substituted
 * directly ({@code sc.cluster_id} -&gt; the grouping-key expression, {@code sc.centroid} -&gt; {@code centroid(vec)} as
 * a per-cluster aggregate, {@code sc.cluster_radius} -&gt; sqrt(max of a pre-group distance binding)). Keeping
 * every post-group descriptor field on the
 * group-aggregation path (rather than constructing a record or indexing {@code C3[cluster_id]}) avoids an
 * optimizer type-inference failure when {@code CLUSTER AS members} is also referenced.
 * <p>
 * Supports a single FROM term (no explicit joins), K-Means only, Euclidean(-squared) distance, and a fixed
 * number of Lloyd iterations. Two init modes are supported: {@code kmeansPP} (the default) runs the exact
 * k-means|| initialization (VLDB'12 "Scalable K-Means++") as the {@code kmeans_oversample_loop} runtime operator
 * -- a seeded-Bernoulli oversampling loop over the global potential, whose pool is then weighted and reduced
 * to {@code k} centroids by {@code kmeans_recluster}; {@code random} skips init and seeds Lloyd from
 * {@code k} uniformly drawn vectors. The WITH options are also validated.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class SqlppClusterByVisitor extends AbstractSqlppSimpleExpressionVisitor {

    // WITH option keys, compared case-insensitively.
    private static final String OPT_ALGORITHM = "clustering_algorithm";
    private static final String OPT_NUM_CLUSTERS = "numclusters";
    private static final String OPT_DISTANCE = "distancefunction";
    private static final String OPT_CROSS_POLLINATION = "crosspollination";
    private static final String OPT_CROSS_POLLINATION_RATIO = "crosspollinationdistanceratio";
    private static final String OPT_INIT_MODE = "initmode";

    private static final Set<String> KNOWN_OPTIONS = Set.of(OPT_ALGORITHM, OPT_NUM_CLUSTERS, OPT_DISTANCE,
            OPT_CROSS_POLLINATION, OPT_CROSS_POLLINATION_RATIO, OPT_INIT_MODE);
    // Only K-Means is supported in this release.
    private static final Set<String> KNOWN_ALGORITHMS = Set.of("k-means", "kmeans");
    // Only the Euclidean family has a matching centroid update: the arithmetic-mean update minimizes
    // squared-Euclidean distance. Cosine/dot would need a normalized-mean (spherical) update to converge,
    // so they are rejected until that is implemented. Names are the builtins VectorDistanceMetric resolves to.
    private static final Set<String> SUPPORTED_DISTANCE_BUILTINS = Set.of(BuiltinFunctions.EUCLIDEAN_DISTANCE.getName(),
            BuiltinFunctions.EUCLIDEAN_SQUARED_DISTANCE.getName());
    // initMode "kmeanspp" (default) = the k-means|| initialization, realized as the paper-faithful (Bahmani et
    // al. VLDB'12, Algorithm 2) exact Bernoulli oversampling: per round a global potential phi = Sigma d^2(x,
    // pool) is reduced and each point is drawn independently with probability p_x = l * d^2(x, pool) / phi. It
    // runs as the self-iterating systolic operator sub-graph (KMEANS_OVERSAMPLE_LOOP).
    // "random" = k uniformly drawn vectors as C0 (Forgy; skip init -- cheap, but seeding quality is what
    // kmeansPP exists for). These are the only two initModes.
    private static final String INIT_MODE_KMEANSPP = "kmeanspp";
    private static final String INIT_MODE_RANDOM = "random";
    private static final Set<String> KNOWN_INIT_MODES = Set.of(INIT_MODE_KMEANSPP, INIT_MODE_RANDOM);
    // Fixed base for the per-round exact-sampling seed (seed_r = EXACT_SEED_BASE + r). Fixed -> a run is
    // reproducible on a fixed topology; the descriptor mixes in the partition id.
    private static final int EXACT_SEED_BASE = 1_000_003;

    // Fixed number of Lloyd iterations (unrolled as nested centroid subqueries).
    private static final int LLOYD_ITERATIONS = 3;

    // k-means|| initialization: oversampling factor l = OVERSAMPLING_FACTOR_PER_K * k, and the number of
    // unrolled oversampling rounds.
    //
    // 5 rounds per the VLDB'12 experiments. Safe ONLY because every centroid-list LET this rewrite emits is
    // marked no-inline (markNoInlineLetVar): each binding compiles once and is shared. With default per-reference
    // inlining, chained rounds grow the plan exponentially (5 rounds formerly put the optimizer fixpoint pass
    // into the hours range).
    private static final int OVERSAMPLING_FACTOR_PER_K = 2;
    private static final int INIT_OVERSAMPLING_ROUNDS = 5;

    private final LangRewritingContext context;

    public SqlppClusterByVisitor(LangRewritingContext context) {
        this.context = context;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        SelectBlock clusterBlock = clusterByBlockOf(selectExpression);
        if (clusterBlock != null) {
            desugarClusterBy(selectExpression, clusterBlock);
        }
        // Recurse (handles nested CLUSTER BY inside subqueries; the emitted subqueries are plain SQL++).
        return super.visit(selectExpression, arg);
    }

    /** The left select block of {@code selectExpression} iff it carries a CLUSTER BY clause; else null. */
    private SelectBlock clusterByBlockOf(SelectExpression selectExpression) {
        SelectSetOperation setOp = selectExpression.getSelectSetOperation();
        SelectBlock leftBlock = blockWithClusterby(setOp.getLeftInput());
        if (leftBlock != null) {
            return leftBlock;
        }
        if (setOp.hasRightInputs()) {
            for (SetOperationRight right : setOp.getRightInputs()) {
                SelectBlock rightBlock = blockWithClusterby(right.getSetOperationRightInput());
                if (rightBlock != null) {
                    return rightBlock;
                }
            }
        }
        return null;
    }

    private static SelectBlock blockWithClusterby(SetOperationInput input) {
        if (!input.selectBlock()) {
            return null;
        }
        SelectBlock selectBlock = input.getSelectBlock();
        return selectBlock != null && selectBlock.hasClusterbyClause() ? selectBlock : null;
    }

    private void desugarClusterBy(SelectExpression selectExpression, SelectBlock selectBlock)
            throws CompilationException {
        ClusterbyClause cbc = selectBlock.getClusterbyClause();
        SourceLocation loc = cbc.getSourceLocation();

        // The grammar already forbids both in one block; guard anyway with a clear message.
        if (selectBlock.hasGroupbyClause()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "A query block may not contain both GROUP BY and CLUSTER BY.");
        }
        if (selectExpression.getSelectSetOperation().hasRightInputs()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY is not supported with set operations (UNION/INTERSECT/EXCEPT).");
        }
        int k = validateWithOptionsAndGetK(cbc);

        // Only a single FROM term is supported (no explicit joins).
        FromClause fromClause = selectBlock.getFromClause();
        if (fromClause == null || fromClause.getFromTerms().size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY currently requires exactly one FROM term.");
        }
        FromTerm fromTerm = fromClause.getFromTerms().get(0);
        if (!fromTerm.getCorrelateClauses().isEmpty() || fromTerm.getLeftVariable() == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY currently does not support joins in its FROM clause.");
        }
        VariableExpr fromVar = fromTerm.getLeftVariable();
        Expression clusteringExpr = cbc.getClusteringExpression();

        // The clustering input is the QUALIFIED block input: the block's WHERE must apply to the centroid
        // pipelines too, not only to the final labeling. WHERE is supported (copied below); LET in the block
        // is rejected (its bindings would need re-scoping into every emitted subquery).
        Expression whereForVecs = null;
        if (selectBlock.hasLetWhereClauses()) {
            for (AbstractClause clause : selectBlock.getLetWhereList()) {
                if (clause instanceof LetClause) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                            "CLUSTER BY currently does not support LET in its query block.");
                }
                Expression wexpr = ((WhereClause) clause).getWhereExpr();
                whereForVecs = whereForVecs == null ? wexpr : binaryOp(OperatorType.AND, whereForVecs, wexpr, loc);
            }
        }

        // __vecs = (FROM <src clone> AS v0 [WHERE <block where[fromVar := v0]>]
        //           SELECT VALUE <clusteringExpr[fromVar := v0]>)
        VariableExpr v0 = newVar(loc);
        Expression srcClone = (Expression) SqlppRewriteUtil.deepCopy(fromTerm.getLeftExpression());
        Expression vecExprForVecs = substitute(clusteringExpr, fromVar, v0, loc);
        Expression whereExprForVecs = whereForVecs == null ? null : substitute(copy(whereForVecs), fromVar, v0, loc);
        VarIdentifier vecs = context.newVariable();
        SelectExpression vecsQuery =
                selectValueFrom(srcClone, v0, vecExprForVecs, null, whereExprForVecs, null, null, null, loc);

        // ---- k-means|| initialization (VLDB'12 "Scalable K-Means++") as runtime operators ----
        List<LetClause> centroidLets = new ArrayList<>();
        centroidLets.add(letClause(vecs, vecsQuery, loc));

        // Init: kmeansPP runs the exact Bernoulli k-means|| oversampling as the runtime systolic loop; random
        // seeds C0 with k uniformly drawn vectors. Both then run Lloyd, all as a linear TOWER of
        // nested internal calls: round r's pool argument IS round r-1's call (the operator echoes its pool
        // through). The subquery args become the operator's stream inputs in the translator; they must be
        // SELF-CONTAINED pipelines (deep copies of the defining subqueries), because the operator's input
        // branches cannot reference the chain's LET vars -- the per-round dataset re-scan this implies is
        // collapsed by the optimizer's common-subtree REPLICATE sharing. Only the final centroid list is
        // LET-bound; intermediate rounds never materialize as arrays.
        boolean randomInit = INIT_MODE_RANDOM.equals(getInitMode(cbc));
        Expression c0Stream;
        if (randomInit) {
            // initMode "random": C0 = k vectors drawn uniformly (Forgy) -- no oversampling stage; the Lloyd
            // stages below are unchanged. Taking the k smallest shuffle keys (see uniformRowKey) is a uniform
            // sample of size k without replacement. Ordering by the vector VALUE instead would return the k
            // lexicographically smallest vectors, i.e. the k most SIMILAR points in the data set rather than a
            // sample of it: all k centroids would sit in one corner, most clusters would start empty, and
            // Lloyd -- which only refines a centroid within its own neighbourhood -- could not recover them.
            // A uniform sample still misses regions sometimes; that is inherent to Forgy, and is why kmeansPP
            // is the default.
            VariableExpr rv0 = newVar(loc);
            LimitClause limitKInit = new LimitClause(intLit(k, loc), null);
            limitKInit.setSourceLocation(loc);
            c0Stream = selectValueFrom(copy(vecsQuery), rv0, rv0, null, null, ascOrder(uniformRowKey(rv0, loc)), null,
                    limitKInit, loc);
        } else {
            // kmeansPP: the self-iterating systolic loop. It loops INIT_OVERSAMPLING_ROUNDS times internally
            // (exact Bernoulli oversampling -- all-reducing the per-round global phi and the drawn candidates),
            // then weighs the residents against the final pool and emits the (count,sum) partials that
            // RECLUSTER reduces. The
            // innermost pool is the single initial centre, which k-means|| draws uniformly at random -- hence
            // the smallest shuffle key (see uniformRowKey) rather than the smallest vector. Ordering by the
            // vector VALUE would always return a geometric extreme, the corner of the data with the smallest
            // leading coordinate. That is the worst available starting point for a D^2 walk, and it biases
            // every round that follows, because each round's draw probabilities are measured from the pool.
            VariableExpr pv = newVar(loc);
            LimitClause seedLimit = new LimitClause(intLit(1, loc), null);
            seedLimit.setSourceLocation(loc);
            Expression poolStream = selectValueFrom(copy(vecsQuery), pv, pv, null, null,
                    ascOrder(uniformRowKey(pv, loc)), null, seedLimit, loc);
            Expression weighed = call(BuiltinFunctions.KMEANS_OVERSAMPLE_LOOP, loc, copy(vecsQuery), poolStream,
                    intLit(OVERSAMPLING_FACTOR_PER_K * k, loc), intLit(INIT_OVERSAMPLING_ROUNDS, loc),
                    intLit(EXACT_SEED_BASE, loc));
            // RECLUSTER: single-input merge of the (broadcast) partials -- emits the k heaviest means (C0),
            // padded from pool members if fewer than k attracted points.
            c0Stream = call(BuiltinFunctions.KMEANS_RECLUSTER, loc, weighed, intLit(k, loc));
        }
        // Lloyd refinement is one self-iterating stage: it loops LLOYD_ITERATIONS times internally, each
        // iteration assigning every vector to its nearest current centroid and all-reducing the per-centroid
        // (count, sum) partials into the next centroid set. A centroid that attracts nothing is dropped, so the
        // set can shrink. Only the final centroid list is LET-bound.
        Expression centroidStream = call(BuiltinFunctions.KMEANS_LLOYD_LOOP, loc, copy(vecsQuery), c0Stream,
                intLit(k, loc), intLit(LLOYD_ITERATIONS, loc));
        VarIdentifier cFinal = context.newVariable();
        centroidLets.add(letClause(cFinal, centroidStream, loc));
        context.markNoInlineLetVar(cFinal);
        VarIdentifier prev = cFinal;
        // The final centroid list is sorted BY VALUE before labeling: the list is otherwise assembled
        // in merge-arrival order, which varies run to run — the cluster PARTITION was already
        // deterministic, but cid labels (indexes into this list) were not. A k-row sort makes the
        // labeling deterministic and identical across the desugar and runtime paths.
        VariableExpr cSortVar = newVar(loc);
        VarIdentifier finalCentroids = context.newVariable();
        centroidLets.add(letClause(finalCentroids, selectValueFrom(varRef(prev, loc), cSortVar, cSortVar, null, null,
                ascOrder(varRef(cSortVar.getVar(), loc)), null, null, loc), loc));
        context.markNoInlineLetVar(finalCentroids);

        // Per-row distance to the assignment centroid, LET-bound in the BLOCK before the GROUP BY:
        // cluster_radius aggregates it with a plain MAX. Binding it pre-group matters twice over: an
        // aggregate whose argument references a variable from OUTSIDE the group (Cfinal) cannot be
        // decomposed into the two-step local/global form, so the group-by would degrade to
        // materializing every group (blowing the operator budget at scale); and the aggregation sugar
        // only accepts aggregate arguments that are group fields, which block LET bindings become.
        // Consequence: when CLUSTER AS is used, member records carry this binding as an extra
        // "__cbdist" field (the standard GROUP AS treatment of block LETs).
        VariableExpr distVar = new VariableExpr(new VarIdentifier("$__cbdist"));
        distVar.setSourceLocation(loc);
        Expression distExpr = call(BuiltinFunctions.NEAREST_CENTROID_DISTANCE, loc, copy(clusteringExpr),
                varRef(finalCentroids, loc));
        LetClause distLet = new LetClause(distVar, distExpr);
        distLet.setSourceLocation(loc);
        List<AbstractClause> letWhere = selectBlock.getLetWhereList();
        letWhere.add(distLet);

        // Convert the block to: GROUP BY nearest_centroid(clusteringExpr, C3) AS $cid [GROUP AS members]
        VariableExpr cidVar = newVar(loc);
        Expression labelExpr =
                call(BuiltinFunctions.NEAREST_CENTROID, loc, clusteringExpr, varRef(finalCentroids, loc));
        // GROUP AS <members>: use its var when present; leave the field list null unless the user gave an explicit
        // member map, so the group-by sugar derives it from the FROM/LET bindings (as for a plain GROUP AS).
        VariableExpr groupVar = cbc.hasClusterMembersVar() ? cbc.getClusterMembersVar() : null;
        List<Pair<Expression, Identifier>> groupFieldList =
                cbc.hasClusterFieldList() ? cbc.getClusterFieldList() : null;
        GroupbyClause mainGby = groupBy(labelExpr, cidVar, groupVar, groupFieldList, loc);

        // Splice into the AST: query-level centroid LETs + GROUP BY on the block.
        selectExpression.getLetList().addAll(centroidLets);
        selectBlock.setClusterbyClause(null);
        selectBlock.setGroupbyClause(mainGby);

        // Replace the descriptor field accesses (sc.cluster_id / sc.centroid / sc.cluster_radius) directly with their
        // values, where <label> is a copy of the grouping-key expression nearest_centroid(vec, C3).
        // substituteGroupbyKeyExpression (which runs next) maps those copies to the group-by key variable.
        // Substituting the field accesses (rather than binding sc to a record) avoids constructing an
        // OpenRecordConstructor whose type inference breaks when the group (members) variable is also referenced.
        // sc.centroid is the mean of the cluster's members: centroid(vec) as a SQL aggregate over the group (the
        // group-by aggregation rewrite turns it into the two-step CENTROID over the group variable). Using this
        // instead of indexing the constant list C3[cluster_id] keeps every post-group descriptor field on the
        // group-aggregation path, avoiding an optimizer type-inference failure when members is also referenced.
        VariableExpr scVar = cbc.getClusterDescriptorVar();
        Map<Expression, Expression> scSubst = new HashMap<>();
        scSubst.put(fieldAccess(scVar, "cluster_id", loc), copy(labelExpr));
        scSubst.put(fieldAccess(scVar, "centroid", loc),
                call(BuiltinFunctions.SCALAR_CENTROID, loc, copy(clusteringExpr)));
        // cluster_radius = sqrt(MAX(distance to the assignment centroid)); MAX is emitted name-based
        // (like count in __wpairs) so the aggregation sugar resolves it over the group, and sqrt is a
        // scalar over the aggregate result (distances are the squared-Euclidean kernel). Measured to
        // the ASSIGNMENT centroid, which may differ from the reported members-mean centroid.
        CallExpr radiusMax =
                new CallExpr(new FunctionSignature(null, null, "max", 1), List.of(new VariableExpr(distVar.getVar())));
        radiusMax.setSourceLocation(loc);
        Expression radiusExpr = call(BuiltinFunctions.NUMERIC_SQRT, loc, radiusMax);
        scSubst.put(fieldAccess(scVar, "cluster_radius", loc), radiusExpr);
        SqlppRewriteUtil.substituteExpression(selectExpression, scSubst, context);
    }

    private FieldAccessor fieldAccess(VariableExpr recordVar, String field, SourceLocation loc) {
        FieldAccessor fa = new FieldAccessor(new VariableExpr(recordVar.getVar()), new Identifier(field));
        fa.setSourceLocation(loc);
        return fa;
    }

    private Expression copy(Expression expr) throws CompilationException {
        return (Expression) SqlppRewriteUtil.deepCopy(expr);
    }

    /** Build {@code SELECT VALUE <valueExpr> FROM <fromSource> AS <fromVar> [LET] [WHERE] [gby] [orderBy] [limit]}. */
    private SelectExpression selectValueFrom(Expression fromSource, VariableExpr fromVar, Expression valueExpr,
            LetClause letBinding, Expression whereExpr, OrderbyClause orderBy, GroupbyClause gby, LimitClause limit,
            SourceLocation loc) {
        FromTerm fromTerm = new FromTerm(fromSource, fromVar, null, null);
        fromTerm.setSourceLocation(loc);
        FromClause fromClause = new FromClause(new ArrayList<>(List.of(fromTerm)));
        fromClause.setSourceLocation(loc);
        SelectElement selectElement = new SelectElement(valueExpr);
        selectElement.setSourceLocation(loc);
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        selectClause.setSourceLocation(loc);
        List<AbstractClause> letWhereList = null;
        if (letBinding != null || whereExpr != null) {
            letWhereList = new ArrayList<>();
            if (letBinding != null) {
                letWhereList.add(letBinding);
            }
            if (whereExpr != null) {
                WhereClause whereClause = new WhereClause(whereExpr);
                whereClause.setSourceLocation(loc);
                letWhereList.add(whereClause);
            }
        }
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, letWhereList, gby, null);
        selectBlock.setSourceLocation(loc);
        SetOperationInput setOpInput = new SetOperationInput(selectBlock, null);
        SelectSetOperation setOp = new SelectSetOperation(setOpInput, null);
        setOp.setSourceLocation(loc);
        SelectExpression selectExpression = new SelectExpression(null, setOp, orderBy, limit, true);
        selectExpression.setSourceLocation(loc);
        return selectExpression;
    }

    private GroupbyClause groupBy(Expression keyExpr, VariableExpr keyVar, VariableExpr groupVar,
            List<Pair<Expression, Identifier>> groupFieldList, SourceLocation loc) {
        GbyVariableExpressionPair pair = new GbyVariableExpressionPair(keyVar, keyExpr);
        List<List<GbyVariableExpressionPair>> gbyList = new ArrayList<>(List.of(new ArrayList<>(List.of(pair))));
        GroupbyClause gby =
                new GroupbyClause(gbyList, new ArrayList<>(), new HashMap<>(), groupVar, groupFieldList, false, false);
        gby.setSourceLocation(loc);
        return gby;
    }

    private LetClause letClause(VarIdentifier var, Expression bindExpr, SourceLocation loc) {
        LetClause let = new LetClause(varRef(var, loc), bindExpr);
        let.setSourceLocation(loc);
        return let;
    }

    private Expression substitute(Expression expr, VariableExpr from, VariableExpr to, SourceLocation loc)
            throws CompilationException {
        Expression copy = (Expression) SqlppRewriteUtil.deepCopy(expr);
        Map<Expression, Expression> subst = new HashMap<>();
        subst.put(from, to);
        return SqlppRewriteUtil.substituteExpression(copy, subst, context);
    }

    private CallExpr call(org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier fid,
            SourceLocation loc, Expression... args) {
        CallExpr call = new CallExpr(new FunctionSignature(fid), new ArrayList<>(Arrays.asList(args)));
        call.setSourceLocation(loc);
        return call;
    }

    private VariableExpr newVar(SourceLocation loc) {
        return varRef(context.newVariable(), loc);
    }

    private VariableExpr varRef(VarIdentifier var, SourceLocation loc) {
        VariableExpr ref = new VariableExpr(var);
        ref.setSourceLocation(loc);
        return ref;
    }

    /** {@code <listExpr>[<idx>]} (constant element index). */
    private Expression elementAt(Expression listExpr, int idx, SourceLocation loc) {
        IndexAccessor ia = new IndexAccessor(listExpr, IndexAccessor.IndexKind.ELEMENT, intLit(idx, loc));
        ia.setSourceLocation(loc);
        return ia;
    }

    /**
     * {@code random(<rowVar>[0])} -- an ORDER BY key that shuffles the vectors rather than ranking them, so
     * that {@code ORDER BY <key> LIMIT n} draws n rows uniformly instead of returning n neighbours. Both init
     * modes need that: seeding k-means from rows selected by their coordinates picks a corner of the data,
     * which is exactly where centroids should not start.
     * <p>
     * {@code random(x)} reseeds its generator whenever its argument differs from the previous call's, so
     * passing a per-row argument yields one draw per seed -- a hash of that row -- where a constant argument
     * would instead walk a single sequence. Consecutive rows with an equal leading coordinate skip the reseed
     * and continue that sequence, so their keys remain distinct but depend on arrival order rather than on the
     * row alone; the sample stays uniform either way, and stays reproducible for a given input order.
     * <p>
     * The key costs nothing: {@code ORDER BY ... LIMIT n} still compiles to a streaming top-n that holds n
     * rows, and ranking one double is cheaper than ranking a vector element by element.
     */
    private Expression uniformRowKey(VariableExpr rowVar, SourceLocation loc) {
        return call(BuiltinFunctions.RANDOM_WITH_SEED, loc, elementAt(varRef(rowVar.getVar(), loc), 0, loc));
    }

    /** {@code <left> <op> <right>} as an OperatorExpr. */
    private Expression binaryOp(OperatorType op, Expression left, Expression right, SourceLocation loc) {
        OperatorExpr oe = new OperatorExpr(new ArrayList<>(List.of(left, right)), new ArrayList<>(List.of(op)), false);
        oe.setSourceLocation(loc);
        return oe;
    }

    private LiteralExpr doubleLit(double v, SourceLocation loc) {
        LiteralExpr lit = new LiteralExpr(new DoubleLiteral(v));
        lit.setSourceLocation(loc);
        return lit;
    }

    /** ORDER BY <expr> ASC (single key, default null order). */
    private static OrderbyClause ascOrder(Expression key) {
        List<OrderbyClause.NullOrderModifier> nullOrder = new ArrayList<>();
        nullOrder.add(null);
        OrderbyClause order = new OrderbyClause(new ArrayList<>(List.of(key)),
                new ArrayList<>(List.of(OrderbyClause.OrderModifier.ASC)), nullOrder);
        order.setSourceLocation(key.getSourceLocation());
        return order;
    }

    private LiteralExpr intLit(int v, SourceLocation loc) {
        LiteralExpr lit = new LiteralExpr(new IntegerLiteral(v));
        lit.setSourceLocation(loc);
        return lit;
    }

    private int validateWithOptionsAndGetK(ClusterbyClause cbc) throws CompilationException {
        Map<String, String> opts = new HashMap<>();
        if (cbc.hasWithOptions()) {
            for (Map.Entry<String, String> e : ConfigurationUtil.toProperties(cbc.getWithOptions()).entrySet()) {
                opts.put(e.getKey().toLowerCase(), e.getValue());
            }
        }
        // Reject unknown keys (catches misspelled option names).
        for (String key : opts.keySet()) {
            if (!KNOWN_OPTIONS.contains(key)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                        "Unknown CLUSTER BY option '" + key + "'. Known options: " + KNOWN_OPTIONS);
            }
        }
        // NumClusters is required and must be a positive integer.
        String numClusters = opts.get(OPT_NUM_CLUSTERS);
        if (numClusters == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                    "CLUSTER BY requires the 'NumClusters' option.");
        }
        int k;
        try {
            k = Integer.parseInt(numClusters.trim());
            if (k <= 0) {
                throw new NumberFormatException(numClusters);
            }
        } catch (NumberFormatException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                    "CLUSTER BY 'NumClusters' must be a positive integer, but was: " + numClusters);
        }
        // distanceFunction is optional. Validate against the engine's shared vector-distance vocabulary and
        // accept only the Euclidean family (see SUPPORTED_DISTANCE_BUILTINS); unknown names and metrics
        // without a matching centroid update (cosine, dot) are both rejected here.
        String distance = opts.get(OPT_DISTANCE);
        if (distance != null) {
            Optional<String> builtin = VectorDistanceMetric.resolve(distance);
            if (builtin.isEmpty() || !SUPPORTED_DISTANCE_BUILTINS.contains(builtin.get())) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                        "CLUSTER BY distanceFunction '" + distance
                                + "' is not supported. Only Euclidean-family metrics are supported: "
                                + "L2, EUCLIDEAN, L2_SQUARED, EUCLIDEAN_SQUARED.");
            }
        }
        // Clustering_Algorithm is optional but, if present, must be supported.
        String algorithm = opts.get(OPT_ALGORITHM);
        if (algorithm != null && !KNOWN_ALGORITHMS.contains(algorithm.toLowerCase())) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                    "Unsupported CLUSTER BY Clustering_Algorithm '" + algorithm + "'. Supported: K-Means.");
        }
        // initMode is optional but, if present, must be recognized.
        String initMode = opts.get(OPT_INIT_MODE);
        if (initMode != null && !KNOWN_INIT_MODES.contains(initMode.toLowerCase())) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                    "Unknown CLUSTER BY initMode '" + initMode + "'. Supported: kmeansPP, random.");
        }
        // CrossPollination / CrossPollinationDistanceRatio are accepted but inert in this release.
        return k;
    }

    /** The validated initMode ({@link #INIT_MODE_KMEANSPP} default). */
    private String getInitMode(ClusterbyClause cbc) throws CompilationException {
        if (cbc.hasWithOptions()) {
            for (Map.Entry<String, String> e : ConfigurationUtil.toProperties(cbc.getWithOptions()).entrySet()) {
                if (OPT_INIT_MODE.equals(e.getKey().toLowerCase())) {
                    return e.getValue().toLowerCase();
                }
            }
        }
        return INIT_MODE_KMEANSPP;
    }
}
