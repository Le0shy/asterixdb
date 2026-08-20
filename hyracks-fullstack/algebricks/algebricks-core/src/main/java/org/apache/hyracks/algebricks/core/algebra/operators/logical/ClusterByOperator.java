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
 * One input: the vectors to cluster. Seeding is part of an algorithm -- how many starting points, drawn
 * how -- so the rule that knows the algorithm derives them from this input rather than the language
 * supplying them.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class ClusterByOperator extends AbstractLogicalOperator {

    // Reference to the vector-valued variable of the input. Held as an EXPRESSION (exposed via
    // acceptExpressionTransform) so variable-substitution and pruning rules see it; a plain LogicalVariable
    // field silently drifts through renames.
    private final Mutable<ILogicalExpression> vectorRef;
    // Reference to the variable holding one member's record -- the row as CLUSTER AS sees it. Built by the
    // translator, which owns the field names the user declared; the expansion only has to listify it.
    private Mutable<ILogicalExpression> memberRecordRef;
    // One tuple per cluster: its id, its centre, how far its furthest member sits from that centre, and the
    // members themselves. This is the whole of what CLUSTER BY means, so nothing downstream has to rebuild
    // any of it -- which is what forced the input to be produced a second time when only centres came out.
    // Types are opaque Objects, supplied by the translator, because the type system lives above Algebricks.
    private LogicalVariable clusterIdVar;
    private LogicalVariable centroidVar;
    private LogicalVariable radiusVar;
    private LogicalVariable membersVar;
    private final Object clusterIdVarType;
    private final Object centroidVarType;
    private final Object radiusVarType;
    private final Object membersVarType;
    // How many clusters the query asked for. Always non-negative.
    private final int numClusters;
    // Which clustering algorithm to run, and how it should seed itself. Both are validated in the rewrite;
    // the physical operator dispatches on them. Strings rather than enums because those vocabularies live
    // above Algebricks.
    private String algorithm;
    private String initMode;
    // Which distance the algorithm measures with, as the metric's canonical name.
    private String metric;

    public ClusterByOperator(Mutable<ILogicalExpression> vectorRef, LogicalVariable clusterIdVar,
            Object clusterIdVarType, LogicalVariable centroidVar, Object centroidVarType, LogicalVariable radiusVar,
            Object radiusVarType, LogicalVariable membersVar, Object membersVarType, int numClusters) {
        this.vectorRef = vectorRef;
        this.clusterIdVar = clusterIdVar;
        this.clusterIdVarType = clusterIdVarType;
        this.centroidVar = centroidVar;
        this.centroidVarType = centroidVarType;
        this.radiusVar = radiusVar;
        this.radiusVarType = radiusVarType;
        this.membersVar = membersVar;
        this.membersVarType = membersVarType;
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
        // A grouping operator: the input tuples are consumed and one tuple per cluster comes out. The rows
        // themselves are not lost -- they are carried in the members list -- so nothing downstream needs the
        // input again, and whatever produced it is read exactly once.
        schema = new ArrayList<>();
        schema.add(clusterIdVar);
        schema.add(centroidVar);
        schema.add(radiusVar);
        schema.add(membersVar);
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                target.addVariable(clusterIdVar);
                target.addVariable(centroidVar);
                target.addVariable(radiusVar);
                target.addVariable(membersVar);
            }
        };
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        // vectorRef is null only for RECLUSTER, the one mode with a pool input and no vector input.
        boolean changed = vectorRef != null && visitor.transform(vectorRef);
        changed |= memberRecordRef != null && visitor.transform(memberRecordRef);
        return changed;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        // Non-propagating, to agree with recomputeSchema and the propagation policy: the input tuples are
        // consumed and only the four cluster variables are live downstream. Same shape as GroupByOperator,
        // whose grouped output likewise replaces its input.
        IVariableTypeEnvironment env =
                new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
        env.setVarType(clusterIdVar, clusterIdVarType);
        env.setVarType(centroidVar, centroidVarType);
        env.setVarType(radiusVar, radiusVarType);
        env.setVarType(membersVar, membersVarType);
        return env;
    }

    /** The vector input variable, or null for RECLUSTER, the only mode without a vector input. */
    public LogicalVariable getVectorVariable() {
        return vectorRef == null ? null : ((VariableReferenceExpression) vectorRef.getValue()).getVariableReference();
    }

    public Mutable<ILogicalExpression> getVectorRef() {
        return vectorRef;
    }

    public Mutable<ILogicalExpression> getMemberRecordRef() {
        return memberRecordRef;
    }

    public void setMemberRecordRef(Mutable<ILogicalExpression> memberRecordRef) {
        this.memberRecordRef = memberRecordRef;
    }

    /** The member-record variable, or null before the translator has set it. */
    public LogicalVariable getMemberRecordVariable() {
        return memberRecordRef == null ? null
                : ((VariableReferenceExpression) memberRecordRef.getValue()).getVariableReference();
    }

    public LogicalVariable getClusterIdVariable() {
        return clusterIdVar;
    }

    public LogicalVariable getCentroidVariable() {
        return centroidVar;
    }

    public LogicalVariable getRadiusVariable() {
        return radiusVar;
    }

    public LogicalVariable getMembersVariable() {
        return membersVar;
    }

    public Object getClusterIdVarType() {
        return clusterIdVarType;
    }

    public Object getCentroidVarType() {
        return centroidVarType;
    }

    public Object getRadiusVarType() {
        return radiusVarType;
    }

    public Object getMembersVarType() {
        return membersVarType;
    }

    public void setClusterIdVariable(LogicalVariable v) {
        this.clusterIdVar = v;
    }

    public void setCentroidVariable(LogicalVariable v) {
        this.centroidVar = v;
    }

    public void setRadiusVariable(LogicalVariable v) {
        this.radiusVar = v;
    }

    public void setMembersVariable(LogicalVariable v) {
        this.membersVar = v;
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
