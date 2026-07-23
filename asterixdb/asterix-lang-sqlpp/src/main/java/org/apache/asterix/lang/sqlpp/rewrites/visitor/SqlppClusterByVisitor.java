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
import org.apache.asterix.lang.common.expression.ListConstructor;
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
 *       __seed = (FROM __vecs AS v SELECT VALUE v LIMIT 1),                     -- k-means|| step 1 (first point)
 *       __cand_r = (FROM __vecs AS v
 *                   WHERE nearest_centroid_distance(v, __pool_{r-1}) &gt; 0
 *                   SELECT VALUE v
 *                   ORDER BY nearest_centroid_distance(v, __pool_{r-1}) DESC
 *                   LIMIT 2k),                                                  -- oversampling rounds r = 1..5
 *       __pool_r = array_concat(__pool_{r-1}, __cand_r),                        -- C &lt;- C u C'  (__pool_0 = __seed)
 *       __wpairs = (FROM __vecs AS v GROUP BY nearest_centroid(v, __pool_R) AS ci
 *                   SELECT VALUE [centroid(v), count(v)]),                      -- step 6: weight the candidates
 *       __top = (FROM __wpairs AS g SELECT VALUE g[0]
 *                ORDER BY g[1] DESC LIMIT k),                                   -- step 7: top-k group means
 *       C0 = (FROM array_concat(__top, __vecs) AS c
 *             SELECT VALUE c LIMIT k),                                          -- pad so |C0| = k
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
 * a per-cluster aggregate, {@code sc.cluster_radius} -&gt; sqrt(max of a pre-group distance binding)). Keeping
 * every post-group descriptor field on the
 * group-aggregation path (rather than constructing a record or indexing {@code C3[cluster_id]}) avoids an
 * optimizer type-inference failure when {@code CLUSTER AS members} is also referenced.
 * <p>
 * Supports a single FROM term (no explicit joins), K-Means only, Euclidean(-squared) distance, and a fixed
 * number of Lloyd iterations. Seeding is the k-means|| initialization (VLDB'12 "Scalable K-Means++"): a
 * deterministic first-point seed, then unrolled top-{@code 2k}-by-distance-to-pool oversampling rounds standing
 * in for the paper's Bernoulli draw (whose expected picks per round is also {@code 2k}); the weighting/recluster
 * steps are approximated by ranking the pool by weight and padding to k. The probabilistic draw with the
 * potential {@code psi}, and sampling or materializing the initialization input, remain future work. The WITH
 * options are also validated.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY -> k-means SQL++ desugar")
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
    // initMode "kmeanspp" (default) = the k-means|| oversampling initialization; "random" = the k
    // lexicographically smallest vectors as C0 (arbitrary-but-DETERMINISTIC; cheap, but seeding
    // quality is exactly what kmeansPP exists for).
    private static final String INIT_MODE_KMEANSPP = "kmeanspp";
    private static final String INIT_MODE_RANDOM = "random";
    // "kmeanspp-exact" = the paper-faithful (Bahmani et al. VLDB'12, Algorithm 2) Bernoulli oversampling:
    // per round a global potential phi = Sigma d^2(x, pool) is reduced and each point is drawn independently
    // with probability p_x = l * d^2(x, pool) / phi. The default "kmeanspp" uses the deterministic top-l
    // approximation of that draw. Exact requires runtime init (the operator tower); it has no desugar form.
    private static final String INIT_MODE_KMEANSPP_EXACT = "kmeanspp-exact";
    // "kmeanspp-exact-loop" = the SAME exact Bernoulli oversampling as "kmeanspp-exact", but realized as a
    // single self-iterating operator (EXPERIMENTAL, single-NC): instead of unrolling the cost/sample rounds
    // into a tower of broadcast-connected operators, the desugar emits ONE kmeans-oversample-loop call that
    // loops internally, all-reducing the per-round global phi and the drawn candidates through an in-operator
    // cross-partition barrier. Draws match "kmeanspp-exact" (same per-round seeds). Runtime-init only.
    private static final String INIT_MODE_KMEANSPP_EXACT_LOOP = "kmeanspp-exact-loop";
    private static final Set<String> KNOWN_INIT_MODES =
            Set.of(INIT_MODE_KMEANSPP, INIT_MODE_RANDOM, INIT_MODE_KMEANSPP_EXACT, INIT_MODE_KMEANSPP_EXACT_LOOP);
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

    // When true (SET `cluster_by_runtime_init` "true"), round 1 of the init is emitted as the internal
    // kmeans-init-candidates(vectors, pool, l) call, realized by the translator as the runtime Store+Score
    // operator; default TRUE: the operator tower is the production path; setting it "false" selects the
    // pure-desugar reference implementation (a debugging/spec tool, slow at scale).
    public static final String CLUSTER_BY_RUNTIME_INIT_OPTION = "cluster_by_runtime_init";

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

        // ---- k-means|| initialization (VLDB'12 "Scalable K-Means++"), steps 1-5 ----
        // Step 1 (deterministic: the smallest vector stands in for a uniform random pick):
        //   __seed = (FROM __vecs AS v SELECT VALUE v LIMIT 1)
        // The seed is the lexicographically SMALLEST vector (ORDER BY the value): a pure function of
        // the data set, so seeding is deterministic across partitioning, arrival order, and restarts.
        // A bare LIMIT 1 raced the partitions through the merge and could pick a different seed run
        // to run. ORDER BY .. LIMIT 1 compiles to a streaming per-partition top-1, not a full sort.
        VariableExpr seedItVar = newVar(loc);
        LimitClause limit1 = new LimitClause(intLit(1, loc), null);
        limit1.setSourceLocation(loc);
        OrderbyClause seedOrder = ascOrder(varRef(seedItVar.getVar(), loc));
        VarIdentifier seed = context.newVariable();
        SelectExpression seedQuery =
                selectValueFrom(varRef(vecs, loc), seedItVar, seedItVar, null, null, seedOrder, null, limit1, loc);

        // Steps 4-5, oversampling round 1 of the hard-wired loop, as one parallel-map + central-reduce round:
        // every partition scores its local points by d2(x, C) and keeps a local top-l (map); the merge keeps the
        // global top-l (reduce). Taking the l = 2k highest-cost points is the deterministic stand-in for the
        // paper's Bernoulli draw with p = l*d2(x,C)/psi, whose expected picks per round is also l; the potential
        // psi returns when the probabilistic draw (or the adaptive round count) is implemented. With a single seed
        // point, d2(x, C) is just the distance to __seed[0]; further rounds grow C and use a distance-to-set
        // score.
        //   __cand = (FROM __vecs AS v SELECT VALUE v
        //             ORDER BY euclidean-squared-distance(v, __seed[0]) DESC LIMIT 2k)
        List<LetClause> centroidLets = new ArrayList<>();
        centroidLets.add(letClause(vecs, vecsQuery, loc));

        // Steps 3-6, the hard-wired oversampling loop: each round is one parallel-map + central-reduce pass.
        // Every partition scores its local points by the distance-to-set d2(x, __pool) and keeps a local top-l
        // (map, a topK sort); the sort-merge keeps the global top-l (reduce); the round's picks join the pool
        // (C <- C u C'). Taking the l = 2k highest-cost points is the deterministic stand-in for the paper's
        // Bernoulli draw with p = l*d2(x,C)/psi, whose expected picks per round is also l; the potential psi
        // returns when the probabilistic draw (or the adaptive round count) is implemented. The d2 > 0 filter is
        // paper-faithful (p = 0 is never drawn) and keeps pool members from being re-sampled. The score is called
        // DIRECTLY in both WHERE and ORDER BY (no LET binding): an ORDER BY on a LET variable defeats the
        // sort+limit topK pushdown, turning each round's local top-l into a FULL external sort of the input
        // (at 100k x 384-dim scale that was ~13 spilling sorts and a cancelled query). The doubled pool
        // reference is harmless now that the pool LETs are compiled once (markNoInlineLetVar).
        //   __cand_r = (FROM __vecs AS v WHERE nearest-centroid-distance(v, __pool_{r-1}) > 0
        //               SELECT VALUE v ORDER BY nearest-centroid-distance(v, __pool_{r-1}) DESC LIMIT 2k)
        //   __pool_r = array_concat(__pool_{r-1}, __cand_r)
        boolean runtimeInit = context.getMetadataProvider() != null
                && context.getMetadataProvider().getBooleanProperty(CLUSTER_BY_RUNTIME_INIT_OPTION, true);
        boolean randomInit = INIT_MODE_RANDOM.equals(getInitMode(cbc));
        boolean exactInit = INIT_MODE_KMEANSPP_EXACT.equals(getInitMode(cbc));
        // Same exact Bernoulli oversampling, but as one self-iterating operator instead of the unrolled tower.
        boolean exactLoop = INIT_MODE_KMEANSPP_EXACT_LOOP.equals(getInitMode(cbc));
        // Exact Bernoulli sampling (either realization) exists only as a runtime operator (the per-round
        // global-phi reduce has no efficient pure-SQL++ desugar). Reject it when runtime init is disabled
        // rather than silently falling back to the deterministic top-l desugar.
        if ((exactInit || exactLoop) && !runtimeInit) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cbc.getSourceLocation(),
                    "CLUSTER BY initMode '" + getInitMode(cbc) + "' requires runtime init; it has no desugar "
                            + "reference path. Enable cluster_by_runtime_init (the default) or use initMode "
                            + "'kmeansPP'.");
        }
        VarIdentifier pool;
        VarIdentifier prev = null;
        if (runtimeInit) {
            // Runtime init: the whole oversampling loop is a linear TOWER of nested calls — round r's pool
            // argument IS round r-1's call (the operator echoes its pool through, so each round's output is
            // the accumulated pool C ∪ C'), and the innermost pool is the seed subquery. The subquery args
            // become the operator's two stream inputs in the translator; they must be SELF-CONTAINED
            // pipelines (deep copies of the defining subqueries), because the operator's input branches are
            // independent trees that cannot reference the chain's LET vars — the per-round dataset re-scan
            // this implies is collapsed by the optimizer's common-subtree REPLICATE sharing. Only the final
            // pool is LET-bound; intermediate rounds never materialize as arrays.
            Expression c0Stream;
            if (randomInit) {
                // initMode "random": C0 = the k lexicographically smallest vectors (deterministic, like
                // the kmeansPP seed) — no oversampling tower; the Lloyd stages below are unchanged.
                VariableExpr rv0 = newVar(loc);
                LimitClause limitKInit = new LimitClause(intLit(k, loc), null);
                limitKInit.setSourceLocation(loc);
                c0Stream = selectValueFrom(copy(vecsQuery), rv0, rv0, null, null, ascOrder(varRef(rv0.getVar(), loc)),
                        null, limitKInit, loc);
            } else {
                VariableExpr pv = newVar(loc);
                LimitClause seedLimit = new LimitClause(intLit(1, loc), null);
                seedLimit.setSourceLocation(loc);
                // Deterministic seed: the lexicographically smallest vector (see the seedQuery comment).
                Expression poolStream = selectValueFrom(copy(vecsQuery), pv, pv, null, null,
                        ascOrder(varRef(pv.getVar(), loc)), null, seedLimit, loc);
                if (exactLoop) {
                    // Single self-iterating operator: it loops INIT_OVERSAMPLING_ROUNDS times internally,
                    // all-reducing phi and the draws through an in-operator barrier, and echoes the final pool.
                    // Same seed base as the unrolled exact tower, so the draws are the same.
                    poolStream = call(BuiltinFunctions.KMEANS_OVERSAMPLE_LOOP, loc, copy(vecsQuery), poolStream,
                            intLit(OVERSAMPLING_FACTOR_PER_K * k, loc), intLit(INIT_OVERSAMPLING_ROUNDS, loc),
                            intLit(EXACT_SEED_BASE, loc));
                } else {
                    for (int r = 0; r < INIT_OVERSAMPLING_ROUNDS; r++) {
                        if (exactInit) {
                            // Paper Algorithm 2 round: COST reduces the global potential phi = Sigma d^2(x,
                            // pool), then SAMPLE draws each point with p_x = l * d^2(x, pool) / phi (distinct
                            // per-round seed for reproducibility). Keeps every draw -- the operator's intake does
                            // not re-limit a SAMPLE's candidates. This is the faithful Bernoulli oversampling;
                            // the deterministic top-l branch below is the approximation.
                            Expression cost = call(BuiltinFunctions.KMEANS_COST, loc, copy(vecsQuery), poolStream,
                                    intLit(OVERSAMPLING_FACTOR_PER_K * k, loc));
                            poolStream = call(BuiltinFunctions.KMEANS_SAMPLE, loc, copy(vecsQuery), cost,
                                    intLit(OVERSAMPLING_FACTOR_PER_K * k, loc), intLit(EXACT_SEED_BASE + r, loc));
                        } else {
                            poolStream = call(BuiltinFunctions.KMEANS_INIT_CANDIDATES, loc, copy(vecsQuery), poolStream,
                                    intLit(OVERSAMPLING_FACTOR_PER_K * k, loc));
                        }
                    }
                }
                // WEIGH: consumes the round tower directly (its intake applies the terminal global
                // re-limit), scores every point once against the decoded pool, and emits per-partition
                // (count, sum) partials per pool member — the runtime realization of the __wpairs GROUP BY.
                Expression weighed = call(BuiltinFunctions.KMEANS_WEIGH_CANDIDATES, loc, copy(vecsQuery), poolStream,
                        intLit(OVERSAMPLING_FACTOR_PER_K * k, loc));
                // RECLUSTER: merges the (broadcast) partials and emits the k heaviest means — C0 — padded
                // from pool members if fewer than k attracted points. Its vector input is unused: LIMIT 1.
                VariableExpr rv = newVar(loc);
                LimitClause reclusterDummyLimit = new LimitClause(intLit(1, loc), null);
                reclusterDummyLimit.setSourceLocation(loc);
                Expression reclusterDummy =
                        selectValueFrom(copy(vecsQuery), rv, rv, null, null, null, null, reclusterDummyLimit, loc);
                c0Stream = call(BuiltinFunctions.KMEANS_RECLUSTER, loc, reclusterDummy, weighed, intLit(k, loc));
            }
            // Lloyd iterations ride the same tower: each is a WEIGH pass over the previous centroids
            // (a plain-vector stream, so no intake re-limit applies) followed by a LLOYD merge emitting
            // every non-empty centroid's mean. Only the final centroid list is LET-bound.
            Expression centroidStream = c0Stream;
            for (int i = 0; i < LLOYD_ITERATIONS; i++) {
                Expression iterWeighed = call(BuiltinFunctions.KMEANS_WEIGH_CANDIDATES, loc, copy(vecsQuery),
                        centroidStream, intLit(k, loc));
                VariableExpr lv = newVar(loc);
                LimitClause lloydDummyLimit = new LimitClause(intLit(1, loc), null);
                lloydDummyLimit.setSourceLocation(loc);
                Expression lloydDummy =
                        selectValueFrom(copy(vecsQuery), lv, lv, null, null, null, null, lloydDummyLimit, loc);
                centroidStream =
                        call(BuiltinFunctions.KMEANS_LLOYD_MERGE, loc, lloydDummy, iterWeighed, intLit(k, loc));
            }
            VarIdentifier cFinal = context.newVariable();
            centroidLets.add(letClause(cFinal, centroidStream, loc));
            context.markNoInlineLetVar(cFinal);
            prev = cFinal;
            pool = null;
        } else {
            if (!randomInit) {
                centroidLets.add(letClause(seed, seedQuery, loc));
                context.markNoInlineLetVar(seed);
            }
            pool = seed;
        }
        int desugarRounds = runtimeInit || randomInit ? 0 : INIT_OVERSAMPLING_ROUNDS;
        for (int r = 0; r < desugarRounds; r++) {
            VariableExpr sampVar = newVar(loc);
            Expression whereScore = call(BuiltinFunctions.NEAREST_CENTROID_DISTANCE, loc, varRef(sampVar.getVar(), loc),
                    varRef(pool, loc));
            Expression positiveScore = binaryOp(OperatorType.GT, whereScore, doubleLit(0.0d, loc), loc);
            Expression orderScore = call(BuiltinFunctions.NEAREST_CENTROID_DISTANCE, loc, varRef(sampVar.getVar(), loc),
                    varRef(pool, loc));
            List<OrderbyClause.NullOrderModifier> defaultNullOrder = new ArrayList<>();
            defaultNullOrder.add(null);
            OrderbyClause topLOrder = new OrderbyClause(new ArrayList<>(List.of(orderScore)),
                    new ArrayList<>(List.of(OrderbyClause.OrderModifier.DESC)), defaultNullOrder);
            topLOrder.setSourceLocation(loc);
            LimitClause limitL = new LimitClause(intLit(OVERSAMPLING_FACTOR_PER_K * k, loc), null);
            limitL.setSourceLocation(loc);
            VarIdentifier cand = context.newVariable();
            centroidLets.add(letClause(cand, selectValueFrom(varRef(vecs, loc), sampVar, sampVar, null, positiveScore,
                    topLOrder, null, limitL, loc), loc));
            context.markNoInlineLetVar(cand);
            VarIdentifier nextPool = context.newVariable();
            centroidLets.add(letClause(nextPool,
                    call(BuiltinFunctions.ARRAY_CONCAT, loc, varRef(pool, loc), varRef(cand, loc)), loc));
            context.markNoInlineLetVar(nextPool);
            pool = nextPool;
        }

        // Steps 6-7: weight every pool candidate by the number of points nearest to it (one more parallel-map +
        // central-reduce round, riding the same two-step GROUP BY machinery as the Lloyd iterations) and keep the
        // k heaviest. Recluster approximation: instead of the paper's central weighted k-means++,
        // rank the candidate groups by weight and emit each group's mean -- a bonus micro-Lloyd step. Two levels,
        // because the aggregation sugar does not resolve aggregates in a SELECT VALUE block's ORDER BY: the inner
        // level emits [mean, weight] pairs, the outer level sorts on the weight element. In runtime-init mode this
        // whole block is realized by the WEIGH + RECLUSTER stages of the operator tower above.
        //   __wpairs = (FROM __vecs AS v GROUP BY nearest_centroid(v, __pool) AS ci
        //               SELECT VALUE [centroid(v), sql-count(v)])
        //   __top = (FROM __wpairs AS g SELECT VALUE g[0] ORDER BY g[1] DESC LIMIT k)
        if (!runtimeInit && randomInit) {
            // initMode "random", reference path: C0 = the k lexicographically smallest vectors.
            VariableExpr rcVar = newVar(loc);
            LimitClause limitKC0 = new LimitClause(intLit(k, loc), null);
            limitKC0.setSourceLocation(loc);
            prev = context.newVariable();
            centroidLets.add(letClause(prev, selectValueFrom(varRef(vecs, loc), rcVar, rcVar, null, null,
                    ascOrder(varRef(rcVar.getVar(), loc)), null, limitKC0, loc), loc));
            context.markNoInlineLetVar(prev);
        }
        if (!runtimeInit && !randomInit) {
            VariableExpr weighVar = newVar(loc);
            Expression weighAssign = call(BuiltinFunctions.NEAREST_CENTROID, loc, weighVar, varRef(pool, loc));
            GroupbyClause weighGby = groupBy(weighAssign, newVar(loc), null, null, loc);
            // count(v) is emitted name-based (as the parser would), NOT via a builtin fid: the group-by aggregation
            // sugar only resolves the parsed form; a direct SCALAR_SQL_COUNT fid dies with "Illegal state:
            // array_sql-count" (centroid tolerates the fid form only because its array_* variants are registered).
            CallExpr countCall = new CallExpr(new FunctionSignature(null, null, "count", 1),
                    List.of(varRef(weighVar.getVar(), loc)));
            countCall.setSourceLocation(loc);
            ListConstructor weighPair = new ListConstructor(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR,
                    new ArrayList<>(List.of(call(BuiltinFunctions.SCALAR_CENTROID, loc, weighVar), countCall)));
            weighPair.setSourceLocation(loc);
            VarIdentifier wpairs = context.newVariable();
            centroidLets.add(letClause(wpairs,
                    selectValueFrom(varRef(vecs, loc), weighVar, weighPair, null, null, null, weighGby, null, loc),
                    loc));
            context.markNoInlineLetVar(wpairs);

            LimitClause limitK = new LimitClause(intLit(k, loc), null);
            limitK.setSourceLocation(loc);
            VariableExpr pairVar = newVar(loc);
            List<OrderbyClause.NullOrderModifier> weightNullOrder = new ArrayList<>();
            weightNullOrder.add(null);
            OrderbyClause byWeight = new OrderbyClause(new ArrayList<>(List.of(elementAt(pairVar, 1, loc))),
                    new ArrayList<>(List.of(OrderbyClause.OrderModifier.DESC)), weightNullOrder);
            byWeight.setSourceLocation(loc);
            VarIdentifier top = context.newVariable();
            centroidLets.add(letClause(top, selectValueFrom(varRef(wpairs, loc), pairVar, elementAt(pairVar, 0, loc),
                    null, null, byWeight, null, limitK, loc), loc));
            context.markNoInlineLetVar(top);

            // Pad with __vecs so C0 always has k entries even when the data has fewer than k occupied candidate groups
            // (duplicates are tolerable: nearest_centroid resolves ties to the first occurrence).
            //   C0 = (FROM array_concat(__top, __vecs) AS c SELECT VALUE c LIMIT k)
            VariableExpr seedVar = newVar(loc);
            LimitClause limitKPad = new LimitClause(intLit(k, loc), null);
            limitKPad.setSourceLocation(loc);
            Expression c0Pool = call(BuiltinFunctions.ARRAY_CONCAT, loc, varRef(top, loc), varRef(vecs, loc));
            SelectExpression c0Query =
                    selectValueFrom(c0Pool, seedVar, seedVar, null, null, null, null, limitKPad, loc);

            prev = context.newVariable();
            centroidLets.add(letClause(prev, c0Query, loc));
            context.markNoInlineLetVar(prev);
        }

        // C1..C3 = (FROM __vecs AS v SELECT VALUE centroid(v) GROUP BY nearest_centroid(v, Cprev))
        // (in runtime-init mode the Lloyd iterations are folded into the operator tower above)
        int lloydIterations = runtimeInit ? 0 : LLOYD_ITERATIONS;
        for (int i = 0; i < lloydIterations; i++) {
            VariableExpr iterVar = newVar(loc);
            Expression assignExpr = call(BuiltinFunctions.NEAREST_CENTROID, loc, iterVar, varRef(prev, loc));
            GroupbyClause gby = groupBy(assignExpr, newVar(loc), null, null, loc);
            Expression centroidExpr = call(BuiltinFunctions.SCALAR_CENTROID, loc, iterVar);
            SelectExpression iterQuery =
                    selectValueFrom(varRef(vecs, loc), iterVar, centroidExpr, null, null, null, gby, null, loc);
            VarIdentifier next = context.newVariable();
            centroidLets.add(letClause(next, iterQuery, loc));
            context.markNoInlineLetVar(next);
            prev = next;
        }
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
                    "Unknown CLUSTER BY initMode '" + initMode + "'. Supported: kmeansPP, kmeansPP-exact, random.");
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
