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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ConfigurationUtil;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.ClusterbyClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.optype.UnnestType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.object.base.AdmArrayNode;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
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
 *   WITH { "num_clusters": k, ... }
 *   SELECT ... sc.cluster_id ... sc.centroid ... sc.cluster_radius ... members ...
 * </pre>
 *
 * becomes (conceptually)
 *
 * <pre>
 *   LET __vecs   = (FROM src AS v SELECT VALUE v.vec),
 *       __weighed = kmeans_oversample_loop(__vecs, __seed, l, rounds, seedBase), -- k-means|| oversampling
 *       C0       = kmeans_recluster(__weighed, k),        -- reduce the weighted candidates to k centres
 *       CFINAL   = kmeans_lloyd_loop(__vecs, C0, k, iterations),
 *       C        = (FROM CFINAL AS c SELECT VALUE c ORDER BY c)   -- so cluster ids do not vary run to run
 *   FROM src AS t
 *   GROUP BY nearest_centroid(t.vec, C) AS $cid [GROUP AS members]
 *   SELECT ...   -- sc.cluster_id -&gt; nearest_centroid(t.vec, C), sc.centroid -&gt; centroid(t.vec),
 *                -- sc.cluster_radius -&gt; sqrt(max(nearest_centroid_distance(t.vec, C)))
 * </pre>
 *
 * {@code init_mode "random"} skips the oversampling/recluster init and seeds Lloyd from {@code k} vectors drawn
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

    // WITH option keys, compared case-insensitively. Named as the vector index's WITH options are (see
    // VectorIndexDeclUtil): num_clusters, dimension and similarity mean the same things there.
    private static final String OPT_ALGORITHM = "clustering_algorithm";
    private static final String OPT_NUM_CLUSTERS = "num_clusters";
    private static final String OPT_SIMILARITY = "similarity";
    private static final String OPT_CROSS_POLLINATION = "cross_pollination";
    private static final String OPT_CROSS_POLLINATION_RATIO = "cross_pollination_distance_ratio";
    private static final String OPT_INIT_MODE = "init_mode";
    private static final String OPT_DIMENSION = "dimension";

    // cross_pollination_distance_ratio is deliberately NOT here: it was accepted but never read, so any value
    // -- negative, non-numeric -- passed silently. It comes back when cross-pollination itself does.
    private static final Set<String> KNOWN_OPTIONS = Set.of(OPT_ALGORITHM, OPT_NUM_CLUSTERS, OPT_SIMILARITY,
            OPT_CROSS_POLLINATION, OPT_INIT_MODE, OPT_DIMENSION);
    // What an unknown-option error lists back to the user. Not printed from KNOWN_OPTIONS: Set.of iterates in
    // a per-JVM salted order, so the message would differ between runs.
    private static final String KNOWN_OPTIONS_DISPLAY =
            "clustering_algorithm, num_clusters, dimension, similarity, cross_pollination, init_mode";
    // The fields the cluster descriptor exposes. Every one is substituted away during the rewrite, so a
    // surviving reference to the descriptor means the query asked for something else -- see checkDescriptorFields.
    private static final String SC_CLUSTER_ID = "cluster_id";
    private static final String SC_CENTROID = "centroid";
    private static final String SC_CLUSTER_RADIUS = "cluster_radius";
    private static final String SC_FIELDS_DISPLAY = SC_CLUSTER_ID + ", " + SC_CENTROID + ", " + SC_CLUSTER_RADIUS;

    // Only K-Means is supported.
    private static final String ALGORITHM_KMEANS = "kmeans";
    private static final Set<String> KNOWN_ALGORITHMS = Set.of("k-means", ALGORITHM_KMEANS);
    // A metric is usable only if some point minimizes total distance to a cluster, since that point is what
    // the update step moves each centroid to: the arithmetic mean for the Euclidean family, that mean
    // projected onto the unit sphere for cosine. Dot is refused -- a negated inner product is unbounded
    // below, so no centroid minimizes it and the update step has nothing to reach.
    private static final Set<VectorSimilarityMetric> SUPPORTED_METRICS = Set.of(VectorSimilarityMetric.EUCLIDEAN,
            VectorSimilarityMetric.EUCLIDEAN_SQUARED, VectorSimilarityMetric.COSINE);
    // Listed back to the user on an unsupported value. Built from the enum so it cannot drift from the check,
    // and sorted so the message does not depend on Set iteration order.
    private static final String SUPPORTED_METRICS_DISPLAY = SUPPORTED_METRICS.stream()
            .map(m -> m.canonical().toUpperCase(Locale.ROOT)).sorted().collect(Collectors.joining(", "));
    // "kmeans_parallel" (default) = k-means|| oversampling, drawing each point with probability
    // p_x = l * d^2(x, pool) / phi. "random" = k uniformly drawn vectors.
    private static final String INIT_MODE_KMEANS_PARALLEL = "kmeans_parallel";
    // The former spelling. It named the inner step -- the reduction really is k-means++ -- rather than the
    // algorithm, which is k-means||. Accepted and canonicalised so queries written against it keep working.
    private static final String INIT_MODE_KMEANSPP_DEPRECATED = "kmeanspp";
    private static final String INIT_MODE_RANDOM = "random";
    private static final Set<String> KNOWN_INIT_MODES =
            Set.of(INIT_MODE_KMEANS_PARALLEL, INIT_MODE_KMEANSPP_DEPRECATED, INIT_MODE_RANDOM);
    // How many clusters is the user's business and is validated here. Everything about how the clustering
    // is carried out -- oversampling width, round count, refinement iterations, the sampling seed, how many
    // starting points -- belongs to the algorithm, and is decided by the rule that expands ClusterByOperator.

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

    /**
     * Turns CLUSTER BY into a clause the translator can build an operator from.
     * <p>
     * The clause stays on the block. What this does is validate the WITH options, put the vector
     * well-formedness test on the block as an ordinary WHERE, and name the four variables the operator
     * produces so the descriptor fields can be pointed at them. Nothing is copied and nothing is rebuilt:
     * the operator consumes the block's own pipeline and emits one tuple per cluster.
     */
    private void desugarClusterBy(SelectExpression selectExpression, SelectBlock selectBlock)
            throws CompilationException {
        ClusterbyClause cbc = selectBlock.getClusterbyClause();
        SourceLocation loc = cbc.getSourceLocation();
        rejectUnsupportedShapes(selectExpression, selectBlock, loc);
        int k = validateWithOptionsAndGetK(cbc);
        int dimension = validateDimensionAndGet(cbc);
        String metric = getMetric(cbc);
        cbc.setResolvedOptions(k, getInitMode(cbc), metric);

        Expression clusteringExpr = cbc.getClusteringExpression();
        // Shape and width, as a WHERE on the block: a row whose vector is absent, null or the wrong length is
        // not a member of any cluster, so it must be gone before the operator sees it rather than dropped
        // inside it. Declared width, not the first row's -- the verdict cannot depend on read order.
        vectorFilter(selectBlock, clusteringExpr, dimension, loc);

        // The operator's output. members always exists, whether or not the query named it: it is what carries
        // the rows through, and the centroid and radius are derived from the same assignment.
        cbc.setClusterIdVar(newVar(loc));
        cbc.setCentroidVar(newVar(loc));
        cbc.setRadiusVar(newVar(loc));
        if (!cbc.hasClusterMembersVar()) {
            cbc.setClusterMembersVar(newVar(loc));
        }
        // What a member looks like: one field per FROM binding, mirroring
        // SqlppGroupByVisitor.createGroupFieldList. LET inside the block is rejected, so the FROM bindings are
        // the whole user-visible set.
        if (!cbc.hasClusterFieldList()) {
            List<Pair<Expression, Identifier>> memberFields = new ArrayList<>();
            for (VariableExpr fromVarExpr : SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause())) {
                SqlppVariableUtil.addToFieldVariableList(fromVarExpr, memberFields);
            }
            cbc.setClusterFieldList(memberFields);
        }

        substituteDescriptorFields(selectExpression, cbc, loc);
    }

    /**
     * Rejects the block shapes the rewrite cannot desugar, and returns the FROM clause the centroid pipelines
     * are built from.
     */
    private FromClause rejectUnsupportedShapes(SelectExpression selectExpression, SelectBlock selectBlock,
            SourceLocation loc) throws CompilationException {
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
        // Several FROM terms and correlate clauses are fine -- inner joins and UNNEST both arrive that way --
        // because the clause is copied wholesale into each operator branch rather than rebuilt from one
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
            // An unmatched row leaves the clustering expression MISSING, and every stage downstream -- the
            // distance, the centroid mean -- assumes a real vector. Which cluster a missing vector belongs to
            // has to be defined before an outer correlate can be accepted. UnnestClause is a sibling of
            // JoinClause rather than a subclass, so each needs its own guard.
            for (AbstractBinaryCorrelateClause correlate : term.getCorrelateClauses()) {
                if (correlate instanceof JoinClause && ((JoinClause) correlate).getJoinType() != JoinType.INNER) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                            "CLUSTER BY currently supports inner joins only; an outer join can leave the "
                                    + "clustering expression MISSING.");
                }
                if (correlate instanceof UnnestClause
                        && ((UnnestClause) correlate).getUnnestType() != UnnestType.INNER) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                            "CLUSTER BY currently supports inner UNNEST only; an outer UNNEST can leave the "
                                    + "clustering expression MISSING.");
                }
            }
        }
        return fromClause;
    }

    /**
     * The predicate the centroid pipelines are filtered by: the block's own WHERE clauses, plus a shape guard
     * on the clustering expression when a Dimension is declared. The shape guard is also appended to the block
     * itself, so the labeling GROUP BY sees exactly the rows the centroids were built from -- filtering only
     * the pipelines would leave the excluded rows to be labelled against centroids built without them, where
     * {@code nearest_centroid} returns NULL and they collect into a (k+1)-th cluster.
     */
    private Expression vectorFilter(SelectBlock selectBlock, Expression clusteringExpr, int dimension,
            SourceLocation loc) throws CompilationException {
        // A block LET cannot be carried across the same way: selectValueFromClause has no LET slot, so a
        // clustering expression naming the LET variable would come out unbound. Supporting it means copying the
        // LETs alongside the WHERE and adding their variables to the group field list, which would also put
        // them in CLUSTER AS members.
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
        if (dimension > 0) {
            // The is-array guard is not redundant: len() RAISES a type mismatch on a string or a number rather
            // than returning unknown, and in an open-type dataset those turn up. Guarded, every bad shape is
            // filtered instead -- absent field, null, wrong type, wrong width.
            Expression widthOk =
                    binaryOp(OperatorType.AND,
                            call(BuiltinFunctions.IS_ARRAY, loc, copy(clusteringExpr)), binaryOp(OperatorType.EQ,
                                    call(BuiltinFunctions.LEN, loc, copy(clusteringExpr)), intLit(dimension, loc), loc),
                            loc);
            whereForVecs = whereForVecs == null ? widthOk
                    : binaryOp(OperatorType.AND, whereForVecs, (Expression) SqlppRewriteUtil.deepCopy(widthOk), loc);
            WhereClause widthFilter = new WhereClause((Expression) SqlppRewriteUtil.deepCopy(widthOk));
            widthFilter.setSourceLocation(loc);
            selectBlock.getLetWhereList().add(widthFilter);
        }
        return whereForVecs;
    }

    /**
     * Points the descriptor's fields at the variables the operator produces.
     * <p>
     * Each is a plain variable reference now. They used to be expressions rebuilt from the centroid list --
     * an aggregate for the centre, a MAX over a per-row distance for the radius -- because the operator
     * emitted neither.
     */
    private void substituteDescriptorFields(SelectExpression selectExpression, ClusterbyClause cbc, SourceLocation loc)
            throws CompilationException {
        VariableExpr scVar = cbc.getClusterDescriptorVar();
        Map<Expression, Expression> scSubst = new HashMap<>();
        scSubst.put(fieldAccess(scVar, SC_CLUSTER_ID, loc), new VariableExpr(cbc.getClusterIdVar().getVar()));
        scSubst.put(fieldAccess(scVar, SC_CENTROID, loc), new VariableExpr(cbc.getCentroidVar().getVar()));
        scSubst.put(fieldAccess(scVar, SC_CLUSTER_RADIUS, loc), new VariableExpr(cbc.getRadiusVar().getVar()));
        SqlppRewriteUtil.substituteExpression(selectExpression, scSubst, context);
        // The clause stays on the block for the translator, and it holds the descriptor variable itself.
        // That is not a reference the user wrote, so drop it before looking for leftovers -- otherwise the
        // check below finds the clause's own field and reports the query as referencing $sc as a value.
        cbc.setClusterDescriptorVar(null);
        // Substitution replaced every field the descriptor actually has. Anything still referring to it is
        // either an unknown field or the descriptor used as a whole value, neither of which survives to
        // runtime -- and left alone both reach the user as a bare "unresolved identifier" naming a variable
        // the rewrite invented. Say what it is instead.
        checkDescriptorResolved(selectExpression, scVar, loc);
    }

    /** Whether the query reads {@code <descriptor>.<field>}, e.g. sc.cluster_radius. */
    private static boolean readsDescriptorField(ILangExpression expr, VariableExpr descriptorVar, String field)
            throws CompilationException {
        DescriptorFieldFinder finder = new DescriptorFieldFinder(descriptorVar, field);
        expr.accept(finder, null);
        return finder.found;
    }

    /**
     * Raises when the query still refers to the cluster descriptor after the rewrite substituted its fields
     * away -- {@code sc.somethingElse}, or {@code sc} on its own. Without this the leftover variable reaches
     * the resolver as an undefined identifier, which names the rewrite's own variable rather than telling the
     * user which field they asked for.
     */
    private static void checkDescriptorResolved(ILangExpression expr, VariableExpr descriptorVar, SourceLocation loc)
            throws CompilationException {
        DescriptorLeftoverFinder finder = new DescriptorLeftoverFinder(descriptorVar);
        expr.accept(finder, null);
        if (finder.found) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY cluster descriptor '" + descriptorVar.getVar().getValue() + "' exposes only "
                            + SC_FIELDS_DISPLAY + ", and cannot be referenced as a whole value.");
        }
    }

    private static final class DescriptorLeftoverFinder extends AbstractSqlppSimpleExpressionVisitor {
        private final VariableExpr descriptorVar;
        private boolean found;

        private DescriptorLeftoverFinder(VariableExpr descriptorVar) {
            this.descriptorVar = descriptorVar;
        }

        @Override
        public Expression visit(VariableExpr v, ILangExpression arg) throws CompilationException {
            if (descriptorVar.getVar().getValue().equals(v.getVar().getValue())) {
                found = true;
            }
            return super.visit(v, arg);
        }
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

    /** {@code <left> <op> <right>} as an OperatorExpr. */
    private Expression binaryOp(OperatorType op, Expression left, Expression right, SourceLocation loc) {
        OperatorExpr oe = new OperatorExpr(new ArrayList<>(List.of(left, right)), new ArrayList<>(List.of(op)), false);
        oe.setSourceLocation(loc);
        return oe;
    }

    private LiteralExpr intLit(int v, SourceLocation loc) {
        LiteralExpr lit = new LiteralExpr(new IntegerLiteral(v));
        lit.setSourceLocation(loc);
        return lit;
    }

    private LiteralExpr strLit(String v, SourceLocation loc) {
        LiteralExpr lit = new LiteralExpr(new StringLiteral(v));
        lit.setSourceLocation(loc);
        return lit;
    }

    /**
     * The scalar WITH options, lower-cased. {@code Dimension} is excluded because it is an array, and
     * {@link ConfigurationUtil#toProperties} rejects every type outside boolean/number/string -- so the whole
     * record has to be walked here rather than flattened wholesale.
     */
    private static Map<String, String> scalarOptions(ClusterbyClause cbc) throws CompilationException {
        Map<String, String> opts = new HashMap<>();
        if (!cbc.hasWithOptions()) {
            return opts;
        }
        for (Map.Entry<String, IAdmNode> e : ExpressionUtils.toNode(cbc.getWithOptions()).getFields()) {
            String key = e.getKey().toLowerCase();
            if (!OPT_DIMENSION.equals(key)) {
                opts.put(key, ConfigurationUtil.getStringValue(e.getValue()));
            }
        }
        return opts;
    }

    /** The raw {@code Dimension} option node, or null when absent. */
    private static IAdmNode dimensionNode(ClusterbyClause cbc) throws CompilationException {
        if (!cbc.hasWithOptions()) {
            return null;
        }
        for (Map.Entry<String, IAdmNode> e : ExpressionUtils.toNode(cbc.getWithOptions()).getFields()) {
            if (OPT_DIMENSION.equals(e.getKey().toLowerCase())) {
                return e.getValue();
            }
        }
        return null;
    }

    private int validateWithOptionsAndGetK(ClusterbyClause cbc) throws CompilationException {
        Map<String, String> opts = scalarOptions(cbc);
        // Reject unknown keys (catches misspelled option names). Dimension is checked separately because
        // scalarOptions() drops it, so a stray "dimensions" would otherwise slip through here.
        Set<String> present = new java.util.HashSet<>(opts.keySet());
        if (dimensionNode(cbc) != null) {
            present.add(OPT_DIMENSION);
        }
        for (String key : present) {
            if (!KNOWN_OPTIONS.contains(key)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                        "Unknown CLUSTER BY option '" + key + "'. Known options: " + KNOWN_OPTIONS_DISPLAY);
            }
        }
        // num_clusters is required and must be a positive integer.
        String numClusters = opts.get(OPT_NUM_CLUSTERS);
        if (numClusters == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                    "CLUSTER BY requires the 'num_clusters' option.");
        }
        int k;
        try {
            k = Integer.parseInt(numClusters.trim());
            if (k <= 0) {
                throw new NumberFormatException(numClusters);
            }
        } catch (NumberFormatException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                    "CLUSTER BY 'num_clusters' must be a positive integer, but was: " + numClusters);
        }
        // Cross-pollination (overlapping clusters) is not implemented, but only a request to turn it ON is an
        // error: false asks for the disjoint clusters this release already produces. Accepting a true would
        // silently hand back disjoint clusters to a query that asked for overlapping ones.
        String crossPollination = opts.get(OPT_CROSS_POLLINATION);
        if (crossPollination != null) {
            String value = crossPollination.trim();
            if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                        "CLUSTER BY 'cross_pollination' must be true or false, but was: " + crossPollination);
            }
            if (Boolean.parseBoolean(value)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                        "CLUSTER BY cross-pollination is currently not enabled; clusters are always disjoint.");
            }
        }
        // similarity is optional, and resolved through the same taxonomy the vector index resolves its own
        // similarity option through. Unknown names and metrics without a matching centroid update (cosine,
        // dot) are both rejected here.
        String similarity = opts.get(OPT_SIMILARITY);
        if (similarity != null) {
            VectorSimilarityMetric metric = VectorSimilarityMetric.fromAlias(similarity);
            if (metric == null || !SUPPORTED_METRICS.contains(metric)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                        "CLUSTER BY 'similarity' '" + similarity + "' is not supported. Supported: "
                                + SUPPORTED_METRICS_DISPLAY + ".");
            }
        }
        // clustering_algorithm is optional but, if present, must be supported.
        String algorithm = opts.get(OPT_ALGORITHM);
        if (algorithm != null && !KNOWN_ALGORITHMS.contains(algorithm.toLowerCase())) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                    "Unsupported CLUSTER BY 'clustering_algorithm' '" + algorithm + "'. Supported: K-Means.");
        }
        // init_mode is optional but, if present, must be recognized.
        String initMode = opts.get(OPT_INIT_MODE);
        if (initMode != null && !KNOWN_INIT_MODES.contains(initMode.toLowerCase())) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                    "Unknown CLUSTER BY 'init_mode' '" + initMode + "'. Supported: kmeans_parallel, random.");
        }
        // cross_pollination_distance_ratio is accepted but inert: it only has meaning once cross-pollination
        // itself is enabled, which the check above guarantees it is not.
        return k;
    }

    /**
     * The validated clustering algorithm, defaulting to k-means. Resolved rather than merely validated because
     * {@code Dimension}'s contract belongs to the algorithm: k-means needs one fixed vector width, and a future
     * algorithm without fixed-width vectors must be able to state its own rule instead of inheriting this one.
     */
    private String getAlgorithm(ClusterbyClause cbc) throws CompilationException {
        String algorithm = scalarOptions(cbc).get(OPT_ALGORITHM);
        return algorithm == null ? ALGORITHM_KMEANS : algorithm.toLowerCase();
    }

    /**
     * The declared vector width. Required for k-means: an open-type dataset carries no schema to infer it from,
     * and inferring it from the first row would make the plan depend on which row happened to arrive first.
     * <p>
     * Typed as an array so that clustering on several fields can declare one width each; k-means clusters a
     * single field (the grammar admits only one clustering expression), so exactly one element is allowed here.
     */
    private int validateDimensionAndGet(ClusterbyClause cbc) throws CompilationException {
        SourceLocation loc = cbc.getSourceLocation();
        IAdmNode node = dimensionNode(cbc);
        String algorithm = getAlgorithm(cbc);
        if (node == null) {
            if (KNOWN_ALGORITHMS.contains(algorithm)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                        "CLUSTER BY with K-Means requires the 'dimension' option: the width of the clustering "
                                + "vector, as a one-element array, e.g. \"Dimension\": [384].");
            }
            return -1; // no other algorithm exists yet; when one does, it states its own requirement here
        }
        if (node.getType() != ATypeTag.ARRAY) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'dimension' must be an array of positive integers, e.g. [384].");
        }
        AdmArrayNode dims = (AdmArrayNode) node;
        if (dims.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY with K-Means clusters a single field, so 'dimension' must hold exactly one "
                            + "element, but held " + dims.size() + ".");
        }
        IAdmNode first = dims.get(0);
        if (first.getType() != ATypeTag.BIGINT) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'dimension' must contain integers, but contained " + first.getType() + ".");
        }
        long dim = ((AdmBigIntNode) first).get();
        if (dim <= 0 || dim > Integer.MAX_VALUE) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'dimension' must be a positive integer, but was: " + dim + ".");
        }
        return (int) dim;
    }

    /** The validated init_mode, canonicalised ({@link #INIT_MODE_KMEANS_PARALLEL} default). */
    private String getInitMode(ClusterbyClause cbc) throws CompilationException {
        String mode = scalarOptions(cbc).get(OPT_INIT_MODE);
        if (mode == null) {
            return INIT_MODE_KMEANS_PARALLEL;
        }
        String lower = mode.toLowerCase();
        return INIT_MODE_KMEANSPP_DEPRECATED.equals(lower) ? INIT_MODE_KMEANS_PARALLEL : lower;
    }

    /**
     * The canonical name of the metric every stage measures with, passed to each of them as a trailing
     * argument. Absent means squared Euclidean.
     * <p>
     * EUCLIDEAN normalizes to EUCLIDEAN_SQUARED: they name the same clustering, since a cluster assignment is
     * an argmin and squaring is monotone, but the oversampling draw probability is defined on d^2, so the two
     * spellings would otherwise sample differently. The squared form is the one both mean.
     */
    private String getMetric(ClusterbyClause cbc) throws CompilationException {
        String similarity = scalarOptions(cbc).get(OPT_SIMILARITY);
        VectorSimilarityMetric metric = similarity == null ? VectorSimilarityMetric.EUCLIDEAN_SQUARED
                : VectorSimilarityMetric.fromAlias(similarity);
        if (metric == VectorSimilarityMetric.EUCLIDEAN) {
            metric = VectorSimilarityMetric.EUCLIDEAN_SQUARED;
        }
        return metric.canonical();
    }
}
