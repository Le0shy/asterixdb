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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * One k-means|| oversampling round (CLUSTER BY initialization): consumes a partitioned vector stream
 * (input 0, materialized once per partition at the physical level) and a small centroid-pool stream
 * (input 1, broadcast), and emits — per partition of input 0 — the {@code topCount} vectors farthest
 * from their nearest pool member (squared Euclidean; score DESC, arrival order on ties; distance-0
 * pool members are never re-emitted). Blocking; produces a single new variable (the candidate vector);
 * input variables are NOT propagated. Semantics are opaque to generic rewrite rules by design: the
 * per-round selection was previously expressed as SELECT/ORDER BY/LIMIT algebra, whose realization
 * regressed with optimizer context (lost topK pushdown, nested-plan in-memory sorts).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY k-means|| init logical operator")
public class KMeansInitCandidatesOperator extends AbstractLogicalOperator {

    /**
     * What this instance computes. ROUND: one oversampling round (local top-{@code topCount} candidates
     * plus the pool echo). FINALIZE: re-limit the intake and unwrap to plain vectors. WEIGH: score every
     * point against the (re-limited) pool and emit per-partition (count, sum) partials per pool member.
     * RECLUSTER: merge the broadcast partials and emit the {@code topCount} heaviest means (C0).
     * LLOYD: merge the broadcast partials and emit EVERY non-empty member's mean in pool order — one
     * Lloyd iteration's recomputed centroids (attracting nothing drops a centroid, as GROUP BY would).
     */
    public enum Mode {
        ROUND,
        FINALIZE,
        WEIGH,
        RECLUSTER,
        LLOYD,
        // Paper-exact Bernoulli oversampling (Bahmani et al. VLDB'12, Algorithm 2), a two-stage round:
        // COST emits each partition's local Sigma d^2(x, pool) as a partial (broadcast -> global phi);
        // SAMPLE draws each vector with p_x = topCount * d^2(x, pool) / phi (seeded), keeping every draw.
        COST,
        SAMPLE
    }

    // References to the vector-valued variable of input 0 (the qualified points) and of input 1 (the
    // pool). Held as EXPRESSIONS (exposed via acceptExpressionTransform) so variable-substitution and
    // pruning rules see them; plain LogicalVariable fields silently drift through renames.
    private final Mutable<ILogicalExpression> vectorRef;
    private final Mutable<ILogicalExpression> poolRef;
    // The single produced variable: a candidate vector, same type as vectorVar (opaque; from translator).
    private LogicalVariable candidateVar;
    private final Object candidateVarType;
    // ROUND/FINALIZE/WEIGH: l = oversampling factor * k (candidates per round / intake re-limit);
    // RECLUSTER: k (means to keep). Always non-negative.
    private final int topCount;
    // Whether input 1 is the output of another tower stage (envelope rows) vs plain vectors (the seed).
    private boolean poolFromPriorRound;
    private Mode mode = Mode.ROUND;
    // Shared-vector materialization (optimization): the vector-reading stages of one tower (ROUND/WEIGH)
    // all read the SAME materialized run file instead of each materializing its own. sharedVectorsKey is a
    // per-tower token (null = not shared, i.e. this stage materializes its own vectors as before); exactly
    // one stage is the writer (materializes under the shared key with sharedConsumerCount readers), the
    // rest drain their vector input and read the shared run file. See the physical operator/descriptor.
    private String sharedVectorsKey;
    private boolean vectorsWriter;
    private int sharedConsumerCount;
    // SAMPLE only: base seed for the per-round, per-partition Bernoulli RNG (reproducible on a fixed
    // topology). keepAllCandidates: exact-path stages (COST + the terminal WEIGH after the last SAMPLE)
    // keep every candidate from the pool input instead of the deterministic top-`topCount` re-limit.
    private long seed;
    private boolean keepAllCandidates;

    public KMeansInitCandidatesOperator(Mutable<ILogicalExpression> vectorRef, Mutable<ILogicalExpression> poolRef,
            LogicalVariable candidateVar, Object candidateVarType, int topCount) {
        this.vectorRef = vectorRef;
        this.poolRef = poolRef;
        this.candidateVar = candidateVar;
        this.candidateVarType = candidateVarType;
        this.topCount = topCount;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.KMEANS_INIT_CANDIDATES;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitKMeansInitCandidatesOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        // Blocking: input 0 is fully materialized before any candidate is emitted.
        return false;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        // Only the candidate variable is live downstream; input tuples are consumed, not propagated.
        schema = new ArrayList<>();
        schema.add(candidateVar);
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                target.addVariable(candidateVar);
            }
        };
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean changed = visitor.transform(vectorRef);
        changed |= visitor.transform(poolRef);
        return changed;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);
        env.setVarType(candidateVar, candidateVarType);
        return env;
    }

    public LogicalVariable getVectorVariable() {
        return ((VariableReferenceExpression) vectorRef.getValue()).getVariableReference();
    }

    public LogicalVariable getPoolVariable() {
        return ((VariableReferenceExpression) poolRef.getValue()).getVariableReference();
    }

    public Mutable<ILogicalExpression> getVectorRef() {
        return vectorRef;
    }

    public Mutable<ILogicalExpression> getPoolRef() {
        return poolRef;
    }

    public LogicalVariable getCandidateVariable() {
        return candidateVar;
    }

    public Object getCandidateVarType() {
        return candidateVarType;
    }

    public void setCandidateVariable(LogicalVariable v) {
        this.candidateVar = v;
    }

    public int getTopCount() {
        return topCount;
    }

    public boolean isPoolFromPriorRound() {
        return poolFromPriorRound;
    }

    public void setPoolFromPriorRound(boolean poolFromPriorRound) {
        this.poolFromPriorRound = poolFromPriorRound;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    /** Per-tower shared-materialization token, or null if this stage materializes its own vectors. */
    public String getSharedVectorsKey() {
        return sharedVectorsKey;
    }

    public void setSharedVectorsKey(String sharedVectorsKey) {
        this.sharedVectorsKey = sharedVectorsKey;
    }

    /** True on the single stage that materializes the shared vector run file for its tower. */
    public boolean isVectorsWriter() {
        return vectorsWriter;
    }

    public void setVectorsWriter(boolean vectorsWriter) {
        this.vectorsWriter = vectorsWriter;
    }

    /** Number of stages that read the shared run file (the writer sets this as the delete refcount). */
    public int getSharedConsumerCount() {
        return sharedConsumerCount;
    }

    public void setSharedConsumerCount(int sharedConsumerCount) {
        this.sharedConsumerCount = sharedConsumerCount;
    }

    /** SAMPLE only: base seed for the per-round, per-partition Bernoulli RNG. */
    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    /** True on exact-path stages that must keep every candidate (no top-`topCount` re-limit). */
    public boolean isKeepAllCandidates() {
        return keepAllCandidates;
    }

    public void setKeepAllCandidates(boolean keepAllCandidates) {
        this.keepAllCandidates = keepAllCandidates;
    }
}
