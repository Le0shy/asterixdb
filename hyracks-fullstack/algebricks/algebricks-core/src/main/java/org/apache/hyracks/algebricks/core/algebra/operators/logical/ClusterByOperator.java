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
import org.apache.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY, as the query expressed it: produces the cluster model -- a stream of centroid vectors -- from a stream of vectors and an
 * initial-centroid stream. Blocking; emits a single new variable and does not propagate its inputs.
 * <p>
 * The node says <em>what</em> to compute: how many clusters, under which metric, by which algorithm. It does
 * not say how, and it has no physical operator of its own. A rewrite rule expands it into the chain of
 * stage operators that implement {@link #getAlgorithm()}, the way the combiner rules expand one group-by
 * into a local and a global one. Adding an algorithm is then a rule, not a job-graph builder.
 * <p>
 * Everything algorithm-specific -- how many oversampling rounds, how wide, how many refinement iterations --
 * is decided by that rule, not here and not in the language layer.
 * <p>
 * Semantics are opaque to generic rewrite rules by design: expressing the algorithm as
 * SELECT/ORDER BY/LIMIT/GROUP BY algebra regressed with optimizer context (lost topK pushdown, nested-plan
 * in-memory sorts).
 * <p>
 * Input 0 is the vectors to cluster; input 1 is the initial centroids. Seeding stays expressible in the
 * query language because a future algorithm may seed differently, and because the expansion rule is free to
 * ignore input 1 when its algorithm seeds itself.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class ClusterByOperator extends AbstractLogicalOperator {

    // Reference to the vector-valued variable of the input. Held as an EXPRESSION (exposed via
    // acceptExpressionTransform) so variable-substitution and pruning rules see it; a plain LogicalVariable
    // field silently drifts through renames.
    private final Mutable<ILogicalExpression> vectorRef;
    private final Mutable<ILogicalExpression> poolRef;
    // The single produced variable: a candidate vector, same type as vectorVar (opaque; from translator).
    private LogicalVariable candidateVar;
    private final Object candidateVarType;
    // How many clusters the query asked for. Always non-negative.
    private final int numClusters;
    // Which clustering algorithm to run, and how it should seed itself. Both are validated in the rewrite;
    // the physical operator dispatches on them. Strings rather than enums because those vocabularies live
    // above Algebricks.
    private String algorithm;
    private String initMode;
    // Which distance the algorithm measures with, as the metric's canonical name.
    private String metric;

    public ClusterByOperator(Mutable<ILogicalExpression> vectorRef, Mutable<ILogicalExpression> poolRef,
            LogicalVariable candidateVar, Object candidateVarType, int numClusters) {
        this.vectorRef = vectorRef;
        this.poolRef = poolRef;
        this.candidateVar = candidateVar;
        this.candidateVarType = candidateVarType;
        this.numClusters = numClusters;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.CLUSTER_BY;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitClusterByOperator(this, arg);
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
        // vectorRef is null only for RECLUSTER, the one mode with a pool input and no vector input.
        boolean changed = vectorRef != null && visitor.transform(vectorRef);
        return changed;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        // Non-propagating, to agree with recomputeSchema and the propagation policy: the input tuples are
        // consumed, and the candidate variable is the only thing live downstream. Propagating the inputs here
        // would advertise types for variables the schema says are gone. Same shape as AggregateOperator.
        IVariableTypeEnvironment env =
                new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
        env.setVarType(candidateVar, candidateVarType);
        return env;
    }

    /** The vector input variable, or null for RECLUSTER, the only mode without a vector input. */
    public LogicalVariable getVectorVariable() {
        return vectorRef == null ? null : ((VariableReferenceExpression) vectorRef.getValue()).getVariableReference();
    }

    public Mutable<ILogicalExpression> getVectorRef() {
        return vectorRef;
    }

    public LogicalVariable getPoolVariable() {
        return ((VariableReferenceExpression) poolRef.getValue()).getVariableReference();
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

    public int getNumClusters() {
        return numClusters;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public String getInitMode() {
        return initMode;
    }

    public void setInitMode(String initMode) {
        this.initMode = initMode;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

}
