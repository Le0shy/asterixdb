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
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
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
 * CLUSTER BY as the query expressed it. Consumes the block's rows and emits one tuple per cluster: its id,
 * its centroid, its radius and its members. Blocking; non-propagating, like GROUP BY.
 * <p>
 * The node says <em>what</em> to compute -- how many clusters, by which algorithm, seeded how, under which
 * metric, at which vector width -- and has no physical operator of its own. Every logical rule sees one opaque
 * node over one ordinary input; a rule at the head of the physical phase expands it into the stages that
 * implement {@link #getAlgorithm()}, the way the combiner rules expand one group-by into a local and a
 * global one. Everything about how the algorithm is carried out is decided by that rule.
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
    // members themselves. Types are opaque Objects, supplied by the translator: the type system lives above
    // Algebricks.
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
    // The declared vector width, enforced by the stages' decoders on the assembled value.
    private int dimension;
    // Computes the members list's type from the member record's once the input is typed: the record's type
    // is known only after type inference, and a list type is an Asterix notion. Typed this way, a consumer of
    // members compiled inside the block sees the same closed record type the expansion's listify produces.
    private IMembersTypeComputer membersTypeComputer;
    // Variables carried through unchanged, as GROUP BY decorations: bound after the operator to the value the
    // expression has before it. The expansion hands them to its labelling GROUP BY.
    private final List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList = new ArrayList<>();

    /** How the members list is typed from one member's record type; implemented above Algebricks. */
    @FunctionalInterface
    public interface IMembersTypeComputer {
        Object membersType(ILogicalExpression memberRecord, IVariableTypeEnvironment inputEnv, ITypingContext ctx)
                throws AlgebricksException;
    }

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
        // Blocking: the input is consumed whole before any cluster is emitted.
        return false;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        // A grouping operator: the input tuples are consumed and one tuple per cluster comes out.
        schema = new ArrayList<>();
        schema.add(clusterIdVar);
        schema.add(centroidVar);
        if (radiusVar != null) {
            schema.add(radiusVar);
        }
        schema.add(membersVar);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            schema.add(p.first);
        }
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                target.addVariable(clusterIdVar);
                target.addVariable(centroidVar);
                if (radiusVar != null) {
                    target.addVariable(radiusVar);
                }
                target.addVariable(membersVar);
                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
                    target.addVariable(p.first);
                }
            }
        };
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        // vectorRef is null only for RECLUSTER, the one mode with a pool input and no vector input.
        boolean changed = vectorRef != null && visitor.transform(vectorRef);
        changed |= memberRecordRef != null && visitor.transform(memberRecordRef);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            changed |= visitor.transform(p.second);
        }
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
        if (radiusVar != null) {
            env.setVarType(radiusVar, radiusVarType);
        }
        env.setVarType(membersVar, membersType(ctx));
        if (!decorList.isEmpty() && !inputs.isEmpty()) {
            IVariableTypeEnvironment inputEnv = ctx.getOutputTypeEnvironment(inputs.get(0).getValue());
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
                env.setVarType(p.first, inputEnv.getType(p.second.getValue()));
            }
        }
        return env;
    }

    private Object membersType(ITypingContext ctx) throws AlgebricksException {
        if (membersTypeComputer == null || memberRecordRef == null || inputs.isEmpty()) {
            return membersVarType;
        }
        IVariableTypeEnvironment inputEnv = ctx.getOutputTypeEnvironment(inputs.get(0).getValue());
        return inputEnv == null ? membersVarType
                : membersTypeComputer.membersType(memberRecordRef.getValue(), inputEnv, ctx);
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

    public IMembersTypeComputer getMembersTypeComputer() {
        return membersTypeComputer;
    }

    public void setMembersTypeComputer(IMembersTypeComputer membersTypeComputer) {
        this.membersTypeComputer = membersTypeComputer;
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

    public List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> getDecorList() {
        return decorList;
    }

    public void addDecorExpression(LogicalVariable variable, ILogicalExpression expression) {
        decorList.add(new Pair<>(variable, new MutableObject<>(expression)));
    }

    /** The decoration variables, in list order. */
    public List<LogicalVariable> getDecorVariables() {
        List<LogicalVariable> vars = new ArrayList<>(decorList.size());
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            vars.add(p.first);
        }
        return vars;
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

    public int getDimension() {
        return dimension;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    /**
     * Whether the query reads the radius. The radius variable exists only then: the operator advertises
     * exactly what its expansion produces, so nothing above it (a CBO sample, a projection) can reference a
     * value that is never computed.
     */
    public boolean isRadiusRead() {
        return radiusVar != null;
    }

}
