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
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ConfigurationUtil;
import org.apache.asterix.lang.common.util.VectorDistanceMetric;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.ClusterbyClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
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
 *       __pool = kmeans_oversample_loop(__vecs, __seed, 2k, 5, seedBase),       -- initMode "kmeansPP":
 *                                                                               --   k-means|| oversampling
 *       C0 = kmeans_recluster(__vecs, __pool, k),                               -- weight the pool, reduce to k
 *       C1 = (FROM __vecs AS v SELECT VALUE centroid(v) GROUP BY nearest_centroid(v, C0)), -- Lloyd iter 1
 *       C2 = (... nearest_centroid(v, C1)),                                                -- iter 2
 *       C3 = (... nearest_centroid(v, C2))                                                 -- iter 3
 *   FROM src AS t
 *   GROUP BY nearest_centroid(t.vec, C3) AS $cid [GROUP AS members]
 *   SELECT ...   -- sc.cluster_id -&gt; nearest_centroid(t.vec, C3), sc.centroid -&gt; centroid(t.vec), radius -&gt; 0.0
 * </pre>
 *
 * {@code initMode "random"} skips the oversampling/recluster init and seeds Lloyd from {@code k} vectors drawn
 * uniformly (Forgy); the Lloyd stage is the same runtime operator either way.
 * <p>
 * The centroid lists are query-level LETs, so the two-step distributed CENTROID aggregate and the
 * {@code nearest_centroid} broadcast labeling come from the downstream group-by / aggregation rewrites. This
 * pass must therefore run BEFORE {@code substituteGroupbyKeyExpression()}/{@code rewriteGroupBys()}, so the
 * GROUP BY it emits is desugared like a parsed one. The descriptor {@code sc} is never materialized: its field
 * accesses are substituted with their values.
 * <p>
 * {@code CLUSTER AS} members are {@code GROUP AS} members -- one field per FROM binding -- with one exception:
 * {@code sc.cluster_radius} aggregates a pre-group distance binding, and only group fields can be aggregated,
 * so a query reading the radius also sees {@code $__cbdist} in its members.
 * <p>
 * Supports inner joins and UNNEST in the FROM clause (outer joins are refused), K-Means only,
 * Euclidean(-squared) distance, a fixed number of Lloyd iterations, and the two init modes above. The WITH
 * options are validated here.
 */
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
    // Only K-Means is supported.
    private static final Set<String> KNOWN_ALGORITHMS = Set.of("k-means", "kmeans");
    // Only the Euclidean family has a matching centroid update: the arithmetic-mean update minimizes
    // squared-Euclidean distance. Cosine/dot would need a normalized-mean (spherical) update to converge,
    // so they are rejected until that is implemented. Names are the builtins VectorDistanceMetric resolves to.
    private static final Set<String> SUPPORTED_DISTANCE_BUILTINS = Set.of(BuiltinFunctions.EUCLIDEAN_DISTANCE.getName(),
            BuiltinFunctions.EUCLIDEAN_SQUARED_DISTANCE.getName());
    // "kmeanspp" (default) = k-means|| oversampling, drawing each point with probability
    // p_x = l * d^2(x, pool) / phi. "random" = k uniformly drawn vectors.
    private static final String INIT_MODE_KMEANSPP = "kmeanspp";
    private static final String INIT_MODE_RANDOM = "random";
    private static final Set<String> KNOWN_INIT_MODES = Set.of(INIT_MODE_KMEANSPP, INIT_MODE_RANDOM);
    // Base for the per-round sampling seed (seed_r = EXACT_SEED_BASE + r). Holding it fixed makes a run
    // reproducible on a fixed topology; the descriptor mixes in the partition id.
    private static final int EXACT_SEED_BASE = 1_000_003;

    // Lloyd iterations, passed to the loop operator as an argument.
    private static final int LLOYD_ITERATIONS = 3;

    // Oversampling factor l = OVERSAMPLING_FACTOR_PER_K * k, over 5 rounds. Safe only because every
    // centroid-list LET here is marked no-inline: under per-reference inlining, chained rounds grow the plan
    // exponentially.
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

        // Mutually exclusive by definition: both decide how the block is grouped, and CLUSTER BY is itself a
        // GROUP BY on the cluster id. The grammar enforces it -- the two are alternatives in SelectBlock.
        if (selectBlock.hasGroupbyClause()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "A query block may not contain both GROUP BY and CLUSTER BY.");
        }
        // This rewrite is per-SelectExpression, not per-branch: clusterByBlockOf returns the first CLUSTER BY
        // block it finds and runs once, so a second branch's clause would survive un-desugared. The centroid
        // LETs also attach to the whole SelectExpression rather than to one branch.
        if (selectExpression.getSelectSetOperation().hasRightInputs()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY is not supported with set operations (UNION/INTERSECT/EXCEPT).");
        }
        int k = validateWithOptionsAndGetK(cbc);

        // Several FROM terms and correlate clauses are fine -- inner joins and UNNEST both arrive that way --
        // because the clause is copied wholesale into each operator branch below rather than rebuilt from one
        // source variable.
        FromClause fromClause = selectBlock.getFromClause();
        // Both are defensive. The grammar requires a FROM clause here, and in practice every term carries a
        // variable: an unaliased source would swallow CLUSTER as its alias and fail to parse.
        if (fromClause == null || fromClause.getFromTerms().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc, "CLUSTER BY requires a FROM clause.");
        }
        for (FromTerm term : fromClause.getFromTerms()) {
            if (term.getLeftVariable() == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                        "CLUSTER BY requires every FROM term to bind a variable.");
            }
            for (AbstractBinaryCorrelateClause correlate : term.getCorrelateClauses()) {
                // An unmatched row leaves the clustering expression MISSING, and every stage downstream --
                // the distance, the centroid mean -- assumes a real vector. Which cluster a missing vector
                // belongs to has to be defined before this can be accepted.
                if (correlate instanceof JoinClause && ((JoinClause) correlate).getJoinType() != JoinType.INNER) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                            "CLUSTER BY currently supports inner joins only; an outer join can leave the "
                                    + "clustering expression MISSING.");
                }
            }
        }
        Expression clusteringExpr = cbc.getClusteringExpression();

        // The centroid pipelines must see the same rows as the labeling, so the block's WHERE is collected
        // here and copied into each of them below. A block LET cannot be carried across the same way:
        // selectValueFromClause has no LET slot, so a clustering expression naming the LET variable would come
        // out unbound. Supporting it means copying the LETs alongside the WHERE and adding their variables to
        // the group field list, which would also put them in CLUSTER AS members.
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

        // __vecs = (FROM <clone of the whole FROM clause> [WHERE <clone of the block WHERE>]
        //           SELECT VALUE <clone of the clustering expression>)
        //
        // Copied whole rather than rebuilt from one source variable, so a join or UNNEST carries all its
        // bindings across. This runs before variable resolution and DeepCopyVisitor keeps the original names,
        // so each copy resolves independently in its own scope.
        FromClause fromCloneForVecs = (FromClause) SqlppRewriteUtil.deepCopy(fromClause);
        Expression vecExprForVecs = (Expression) SqlppRewriteUtil.deepCopy(clusteringExpr);
        Expression whereExprForVecs =
                whereForVecs == null ? null : (Expression) SqlppRewriteUtil.deepCopy(whereForVecs);
        VarIdentifier vecs = context.newVariable();
        SelectExpression vecsQuery = selectValueFromClause(fromCloneForVecs, vecExprForVecs, whereExprForVecs, loc);

        // k-means|| initialization, run as operators.
        List<LetClause> centroidLets = new ArrayList<>();
        centroidLets.add(letClause(vecs, vecsQuery, loc));

        // Init then Lloyd, chained by nesting each call inside the next: RECLUSTER consumes the oversample
        // loop, and the Lloyd loop consumes RECLUSTER. Rounds are an argument to each loop, not unrolled here.
        // The subquery arguments become the operators' stream inputs, so they must be self-contained pipelines
        // -- an input branch cannot reference the chain's LET vars. The repeated scans this implies are
        // collapsed by the optimizer's common-subtree REPLICATE sharing.
        boolean randomInit = INIT_MODE_RANDOM.equals(getInitMode(cbc));
        Expression c0Stream;
        if (randomInit) {
            // C0 = k vectors drawn uniformly (Forgy). The k smallest shuffle keys (uniformRowKey) are a uniform
            // sample without replacement; ordering by the vector VALUE would instead return the k most similar
            // points, seating every centroid in one corner where Lloyd cannot recover them.
            VariableExpr rv0 = newVar(loc);
            LimitClause limitKInit = new LimitClause(intLit(k, loc), null);
            limitKInit.setSourceLocation(loc);
            c0Stream = selectValueFrom(copy(vecsQuery), rv0, rv0, null, null, ascOrder(uniformRowKey(rv0, loc)), null,
                    limitKInit, loc);
        } else {
            // The oversampling loop runs INIT_OVERSAMPLING_ROUNDS rounds internally, then weighs the vectors
            // against the final pool into the (count, sum) partials RECLUSTER reduces. The innermost pool is
            // the single initial centre, drawn uniformly -- hence the smallest shuffle key, not the smallest
            // vector, which would be a geometric extreme and bias every round measured from it.
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
        // Lloyd refinement loops LLOYD_ITERATIONS times inside one stage, all-reducing the per-centroid
        // (count, sum) partials each iteration. A centroid that attracts nothing is dropped, so k can shrink.
        Expression centroidStream = call(BuiltinFunctions.KMEANS_LLOYD_LOOP, loc, copy(vecsQuery), c0Stream,
                intLit(k, loc), intLit(LLOYD_ITERATIONS, loc));
        VarIdentifier cFinal = context.newVariable();
        centroidLets.add(letClause(cFinal, centroidStream, loc));
        context.markNoInlineLetVar(cFinal);
        VarIdentifier prev = cFinal;
        // Sorted by value before labeling. The partition was already deterministic, but the list arrived in
        // merge order, which varies run to run -- so the cid labels, being indexes into it, did not.
        VariableExpr cSortVar = newVar(loc);
        VarIdentifier finalCentroids = context.newVariable();
        centroidLets.add(letClause(finalCentroids, selectValueFrom(varRef(prev, loc), cSortVar, cSortVar, null, null,
                ascOrder(varRef(cSortVar.getVar(), loc)), null, null, loc), loc));
        context.markNoInlineLetVar(finalCentroids);

        // Per-row distance to the assignment centroid, bound in the block before the GROUP BY so that the MAX
        // behind cluster_radius stays a two-step local/global aggregate: an aggregate over Cfinal, a variable
        // from outside the group, cannot decompose and would materialize every group. Only group fields can be
        // aggregated, and group fields are what CLUSTER AS members are made of -- so this is bound only when
        // the query reads cluster_radius, leaving members clean otherwise.
        boolean usesRadius = readsDescriptorField(selectExpression, cbc.getClusterDescriptorVar(), "cluster_radius");
        VariableExpr distVar = new VariableExpr(new VarIdentifier("$__cbdist"));
        distVar.setSourceLocation(loc);
        Expression distExpr = call(BuiltinFunctions.NEAREST_CENTROID_DISTANCE, loc, copy(clusteringExpr),
                varRef(finalCentroids, loc));
        LetClause distLet = new LetClause(distVar, distExpr);
        distLet.setSourceLocation(loc);
        List<AbstractClause> letWhere = selectBlock.getLetWhereList();
        if (usesRadius) {
            letWhere.add(distLet);
        }

        // Convert the block to: GROUP BY nearest_centroid(clusteringExpr, C3) AS $cid [GROUP AS members]
        VariableExpr cidVar = newVar(loc);
        Expression labelExpr =
                call(BuiltinFunctions.NEAREST_CENTROID, loc, clusteringExpr, varRef(finalCentroids, loc));
        // The field list mirrors SqlppGroupByVisitor.createGroupFieldList: the FROM bindings, which are the
        // whole user-visible set since LET in a CLUSTER BY block is rejected, plus $__cbdist when the radius
        // needs it as a group field.
        VariableExpr groupVar = cbc.hasClusterMembersVar() ? cbc.getClusterMembersVar() : null;
        List<Pair<Expression, Identifier>> groupFieldList = null;
        if (cbc.hasClusterFieldList()) {
            groupFieldList = cbc.getClusterFieldList();
        } else if (groupVar != null) {
            groupFieldList = new ArrayList<>();
            for (VariableExpr fromVarExpr : SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause())) {
                SqlppVariableUtil.addToFieldVariableList(fromVarExpr, groupFieldList);
            }
            if (usesRadius) {
                SqlppVariableUtil.addToFieldVariableList(distVar, groupFieldList);
            }
        }
        GroupbyClause mainGby = groupBy(labelExpr, cidVar, groupVar, groupFieldList, loc);

        // Splice into the AST: query-level centroid LETs + GROUP BY on the block.
        selectExpression.getLetList().addAll(centroidLets);
        selectBlock.setClusterbyClause(null);
        selectBlock.setGroupbyClause(mainGby);

        // Substitute the descriptor's field accesses with their values rather than binding sc to a record: an
        // OpenRecordConstructor here breaks type inference when the members variable is also referenced. For
        // the same reason sc.centroid becomes centroid(vec) as a group aggregate rather than C3[cluster_id],
        // keeping every post-group descriptor field on the group-aggregation path.
        VariableExpr scVar = cbc.getClusterDescriptorVar();
        Map<Expression, Expression> scSubst = new HashMap<>();
        scSubst.put(fieldAccess(scVar, "cluster_id", loc), copy(labelExpr));
        scSubst.put(fieldAccess(scVar, "centroid", loc),
                call(BuiltinFunctions.SCALAR_CENTROID, loc, copy(clusteringExpr)));
        // cluster_radius = sqrt(MAX(distance)); MAX is emitted name-based so the aggregation sugar resolves it
        // over the group. Measured to the ASSIGNMENT centroid, which may differ from the reported centroid.
        CallExpr radiusMax =
                new CallExpr(new FunctionSignature(null, null, "max", 1), List.of(new VariableExpr(distVar.getVar())));
        radiusMax.setSourceLocation(loc);
        Expression radiusExpr = call(BuiltinFunctions.NUMERIC_SQRT, loc, radiusMax);
        if (usesRadius) {
            scSubst.put(fieldAccess(scVar, "cluster_radius", loc), radiusExpr);
        }
        SqlppRewriteUtil.substituteExpression(selectExpression, scSubst, context);
    }

    /** Whether the query reads {@code <descriptor>.<field>}, e.g. sc.cluster_radius. */
    private static boolean readsDescriptorField(ILangExpression expr, VariableExpr descriptorVar, String field)
            throws CompilationException {
        DescriptorFieldFinder finder = new DescriptorFieldFinder(descriptorVar, field);
        expr.accept(finder, null);
        return finder.found;
    }

    private static final class DescriptorFieldFinder extends AbstractSqlppSimpleExpressionVisitor {
        private final VariableExpr descriptorVar;
        private final String field;
        private boolean found;

        private DescriptorFieldFinder(VariableExpr descriptorVar, String field) {
            this.descriptorVar = descriptorVar;
            this.field = field;
        }

        @Override
        public Expression visit(FieldAccessor fa, ILangExpression arg) throws CompilationException {
            if (field.equals(fa.getIdent().getValue()) && fa.getExpr().getKind() == Expression.Kind.VARIABLE_EXPRESSION
                    && descriptorVar.getVar().getValue().equals(((VariableExpr) fa.getExpr()).getVar().getValue())) {
                found = true;
            }
            return super.visit(fa, arg);
        }
    }

    private FieldAccessor fieldAccess(VariableExpr recordVar, String field, SourceLocation loc) {
        FieldAccessor fa = new FieldAccessor(new VariableExpr(recordVar.getVar()), new Identifier(field));
        fa.setSourceLocation(loc);
        return fa;
    }

    private Expression copy(Expression expr) throws CompilationException {
        return (Expression) SqlppRewriteUtil.deepCopy(expr);
    }

    /**
     * {@code (FROM <fromClause> [WHERE <whereExpr>] SELECT VALUE <valueExpr>)} over a ready-made FROM clause,
     * for inputs that bind more than one variable. {@link #selectValueFrom} is the single-source form.
     */
    private SelectExpression selectValueFromClause(FromClause fromClause, Expression valueExpr, Expression whereExpr,
            SourceLocation loc) {
        SelectElement selectElement = new SelectElement(valueExpr);
        selectElement.setSourceLocation(loc);
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        selectClause.setSourceLocation(loc);
        List<AbstractClause> letWhereList = null;
        if (whereExpr != null) {
            WhereClause whereClause = new WhereClause(whereExpr);
            whereClause.setSourceLocation(loc);
            letWhereList = new ArrayList<>(List.of(whereClause));
        }
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, letWhereList, null, null);
        selectBlock.setSourceLocation(loc);
        SetOperationInput setOpInput = new SetOperationInput(selectBlock, null);
        SelectSetOperation setOp = new SelectSetOperation(setOpInput, null);
        setOp.setSourceLocation(loc);
        SelectExpression selectExpression = new SelectExpression(null, setOp, null, null, true);
        selectExpression.setSourceLocation(loc);
        return selectExpression;
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

    private CallExpr call(FunctionIdentifier fid, SourceLocation loc, Expression... args) {
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
        // Cross-pollination (overlapping clusters) is not implemented, but only a request to turn it ON is an
        // error: false asks for the disjoint clusters this release already produces. Accepting a true would
        // silently hand back disjoint clusters to a query that asked for overlapping ones.
        String crossPollination = opts.get(OPT_CROSS_POLLINATION);
        if (crossPollination != null) {
            String value = crossPollination.trim();
            if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                        "CLUSTER BY 'CrossPollination' must be true or false, but was: " + crossPollination);
            }
            if (Boolean.parseBoolean(value)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                        "CLUSTER BY cross-pollination is currently not enabled; clusters are always disjoint.");
            }
        }
        // distanceFunction is optional. Only the Euclidean family is accepted: unknown names, and metrics
        // without a matching centroid update (cosine, dot), are both rejected here.
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
        // CrossPollinationDistanceRatio is accepted but inert: it only has meaning once cross-pollination
        // itself is enabled, which the check above guarantees it is not.
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
