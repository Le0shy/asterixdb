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
 *   LET __vecs = (FROM src AS v [WHERE block-where] SELECT VALUE v.vec),
 *       C0 = (FROM __vecs AS v SELECT VALUE v LIMIT k),                                    -- first-k seed
 *       C1 = (FROM __vecs AS v SELECT VALUE centroid(v) GROUP BY nearest_centroid(v, C0)), -- Lloyd iter 1
 *       C2 = (... nearest_centroid(v, C1)),                                                -- iter 2
 *       C3 = (... nearest_centroid(v, C2))                                                 -- iter 3
 *   FROM src AS t
 *   GROUP BY nearest_centroid(t.vec, C3) AS $cid [GROUP AS members]
 *   SELECT ...   -- sc.cluster_id -&gt; nearest_centroid(t.vec, C3), sc.centroid -&gt; centroid(t.vec), radius -&gt; 0.0
 * </pre>
 *
 * The centroid lists {@code C0..C3} are query-level LETs (constants, in scope after the GROUP BY). The two-step
 * distributed CENTROID aggregate + {@code nearest_centroid} broadcast labeling are supplied by the downstream
 * group-by / aggregation rewrites, so this pass rides the normal SQL++ pipeline. It must run BEFORE
 * {@code substituteGroupbyKeyExpression()}/{@code rewriteGroupBys()} so the emitted GROUP BY is desugared like a
 * parsed one. The descriptor {@code sc} is not a materialized variable: its field accesses are substituted
 * directly ({@code sc.cluster_id} -&gt; the grouping-key expression, {@code sc.centroid} -&gt; {@code centroid(vec)} as
 * a per-cluster aggregate, {@code sc.cluster_radius} -&gt; 0.0). Keeping every post-group descriptor field on the
 * group-aggregation path (rather than constructing a record or indexing {@code C3[cluster_id]}) avoids an
 * optimizer type-inference failure when {@code CLUSTER AS members} is also referenced.
 * <p>
 * Scope: a single FROM term (no explicit joins), K-Means only, Euclidean(-squared) distance, a first-k seed,
 * and a fixed number of Lloyd iterations. {@code cluster_radius} is 0.0 in this version; a real max-distance
 * radius is a future enhancement. The WITH options are also validated.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY -> k-means SQL++ desugar")
public class SqlppClusterByVisitor extends AbstractSqlppSimpleExpressionVisitor {

    // WITH option keys, compared case-insensitively.
    private static final String OPT_ALGORITHM = "clustering_algorithm";
    private static final String OPT_NUM_CLUSTERS = "numclusters";
    private static final String OPT_DISTANCE = "distancefunction";
    private static final String OPT_CROSS_POLLINATION = "crosspollination";
    private static final String OPT_CROSS_POLLINATION_RATIO = "crosspollinationdistanceratio";

    private static final Set<String> KNOWN_OPTIONS =
            Set.of(OPT_ALGORITHM, OPT_NUM_CLUSTERS, OPT_DISTANCE, OPT_CROSS_POLLINATION, OPT_CROSS_POLLINATION_RATIO);
    // Only K-Means is supported in this release.
    private static final Set<String> KNOWN_ALGORITHMS = Set.of("k-means", "kmeans");
    // Only the Euclidean family has a matching centroid update: the arithmetic-mean update minimizes
    // squared-Euclidean distance. Cosine/dot would need a normalized-mean (spherical) update to converge,
    // so they are rejected until that is implemented. Names are the builtins VectorDistanceMetric resolves to.
    private static final Set<String> SUPPORTED_DISTANCE_BUILTINS = Set.of(BuiltinFunctions.EUCLIDEAN_DISTANCE.getName(),
            BuiltinFunctions.EUCLIDEAN_SQUARED_DISTANCE.getName());

    // Fixed number of Lloyd iterations (unrolled as nested centroid subqueries).
    private static final int LLOYD_ITERATIONS = 3;

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

    /**
     * The select block of {@code selectExpression} that carries a CLUSTER BY clause, checked across ALL
     * set-operation inputs (left and right), else null. Checking every input matters: a CLUSTER BY in a
     * right UNION/INTERSECT/EXCEPT branch must be found so {@code desugarClusterBy} runs and its set-op
     * guard rejects it — otherwise the clause would be silently ignored.
     */
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

        // The block's WHERE must apply to the centroid pipelines too, not only to the final labeling,
        // otherwise centroids would train on unfiltered data. LET in the block is rejected: its bindings
        // would have to be re-scoped into every emitted subquery.
        Expression whereForVecs = null;
        if (selectBlock.hasLetWhereClauses()) {
            for (AbstractClause clause : selectBlock.getLetWhereList()) {
                if (clause instanceof LetClause) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                            "CLUSTER BY does not support LET in its query block in this version.");
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

        // C0 = (FROM __vecs AS v SELECT VALUE v LIMIT k)   -- first-k seed centroids
        VariableExpr seedVar = newVar(loc);
        LimitClause limitK = new LimitClause(intLit(k, loc), null);
        limitK.setSourceLocation(loc);
        SelectExpression c0Query =
                selectValueFrom(varRef(vecs, loc), seedVar, seedVar, null, null, null, null, limitK, loc);

        List<LetClause> centroidLets = new ArrayList<>();
        centroidLets.add(letClause(vecs, vecsQuery, loc));
        VarIdentifier prev = context.newVariable();
        centroidLets.add(letClause(prev, c0Query, loc));

        // C1..C3 = (FROM __vecs AS v SELECT VALUE centroid(v) GROUP BY nearest_centroid(v, Cprev))
        for (int i = 0; i < LLOYD_ITERATIONS; i++) {
            VariableExpr iterVar = newVar(loc);
            Expression assignExpr = call(BuiltinFunctions.NEAREST_CENTROID, loc, iterVar, varRef(prev, loc));
            GroupbyClause gby = groupBy(assignExpr, newVar(loc), null, null, loc);
            Expression centroidExpr = call(BuiltinFunctions.SCALAR_CENTROID, loc, iterVar);
            SelectExpression iterQuery =
                    selectValueFrom(varRef(vecs, loc), iterVar, centroidExpr, null, null, null, gby, null, loc);
            VarIdentifier next = context.newVariable();
            centroidLets.add(letClause(next, iterQuery, loc));
            prev = next;
        }
        VarIdentifier finalCentroids = prev;

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
        LiteralExpr zero = new LiteralExpr(new DoubleLiteral(0.0d));
        zero.setSourceLocation(loc);
        Map<Expression, Expression> scSubst = new HashMap<>();
        scSubst.put(fieldAccess(scVar, "cluster_id", loc), copy(labelExpr));
        scSubst.put(fieldAccess(scVar, "centroid", loc),
                call(BuiltinFunctions.SCALAR_CENTROID, loc, copy(clusteringExpr)));
        scSubst.put(fieldAccess(scVar, "cluster_radius", loc), zero); // 0.0 in this version; real radius later.
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

    /** Build {@code SELECT VALUE <valueExpr> FROM <fromSource> AS <fromVar> [gby] [limit]} as a subquery. */
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

    /** {@code <left> <op> <right>} as an OperatorExpr. */
    private Expression binaryOp(OperatorType op, Expression left, Expression right, SourceLocation loc) {
        OperatorExpr oe = new OperatorExpr(new ArrayList<>(List.of(left, right)), new ArrayList<>(List.of(op)), false);
        oe.setSourceLocation(loc);
        return oe;
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
        // CrossPollination / CrossPollinationDistanceRatio are accepted but inert in this release.
        return k;
    }
}
