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
package org.apache.asterix.lang.sqlpp.clause;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * The SQL++ {@code CLUSTER BY} clause: similarity-based, fuzzy analog of {@code GROUP BY} over a
 * vector expression. {@code CLUSTER BY <expr> AS <descriptor> [CLUSTER AS <members>] WITH {...}}.
 */
public class ClusterbyClause extends AbstractClause {

    // The vector expression to cluster on.
    private Expression clusteringExpr;
    // The cluster descriptor variable (AS sc): {cluster_id, centroid, cluster_radius}.
    private VariableExpr clusterDescriptorVar;
    // The cluster-members variable (CLUSTER AS <var>); null when CLUSTER AS is absent.
    private VariableExpr clusterMembersVar;
    // Field mapping for the cluster members; empty when CLUSTER AS is absent or has no explicit map.
    private List<Pair<Expression, Identifier>> clusterFieldList = new ArrayList<>();
    // Raw WITH options (algorithm, k, distance, ...); validated/extracted later by the rewrite pass.
    private RecordConstructor withOptions;

    // Filled in by SqlppClusterByVisitor once the WITH options have been validated, and read by the
    // translator. The clause reaches the translator carrying its own settings rather than the translator
    // re-deriving them, so validation stays in the language layer where its error messages belong.
    private int numClusters;
    private String initMode;
    private String metric;
    // The declared vector width, enforced by the stages' decoders on the assembled value rather than as a
    // predicate on the block (the columnar filter pushdown would evaluate it per array element).
    private int dimension;
    // Whether the query reads sc.cluster_radius; the expansion builds the distance and its aggregate only then.
    private boolean radiusRead;
    // The clustering expression as the user wrote it, for messages about rows it does not describe.
    private String clusteringExpressionText;
    // The variables the CLUSTER BY operator produces, one tuple per cluster: id, centroid, radius, members.
    // Named by the rewrite so the descriptor's fields can be pointed at them before the translator binds them
    // -- the hand-off GroupbyClause makes for its grouping variables.
    // Variables live before the clause that the query reads after it, carried through the operator unchanged,
    // as GROUP BY decorations are. Filled by the aggregation sugar visitor, after name resolution.
    private List<GbyVariableExpressionPair> decorPairList;
    private VariableExpr clusterIdVar;
    private VariableExpr centroidVar;
    private VariableExpr radiusVar;

    public ClusterbyClause(Expression clusteringExpr, VariableExpr clusterDescriptorVar, VariableExpr clusterMembersVar,
            List<Pair<Expression, Identifier>> clusterFieldList, RecordConstructor withOptions) {
        this.clusteringExpr = clusteringExpr;
        this.clusterDescriptorVar = clusterDescriptorVar;
        this.clusterMembersVar = clusterMembersVar;
        if (clusterFieldList != null) {
            this.clusterFieldList = clusterFieldList;
        }
        this.withOptions = withOptions;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.CLUSTER_BY_CLAUSE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    public Expression getClusteringExpression() {
        return clusteringExpr;
    }

    public void setClusteringExpression(Expression clusteringExpr) {
        this.clusteringExpr = clusteringExpr;
    }

    public VariableExpr getClusterDescriptorVar() {
        return clusterDescriptorVar;
    }

    public VariableExpr getClusterMembersVar() {
        return clusterMembersVar;
    }

    public void setClusterMembersVar(VariableExpr clusterMembersVar) {
        this.clusterMembersVar = clusterMembersVar;
    }

    public List<Pair<Expression, Identifier>> getClusterFieldList() {
        return clusterFieldList;
    }

    public void setClusterFieldList(List<Pair<Expression, Identifier>> clusterFieldList) {
        if (clusterFieldList != null) {
            this.clusterFieldList = clusterFieldList;
        }
    }

    public RecordConstructor getWithOptions() {
        return withOptions;
    }

    public void setWithOptions(RecordConstructor withOptions) {
        this.withOptions = withOptions;
    }

    public int getNumClusters() {
        return numClusters;
    }

    public String getInitMode() {
        return initMode;
    }

    public String getMetric() {
        return metric;
    }

    public int getDimension() {
        return dimension;
    }

    public boolean isRadiusRead() {
        return radiusRead;
    }

    public void setRadiusRead(boolean radiusRead) {
        this.radiusRead = radiusRead;
    }

    public String getClusteringExpressionText() {
        return clusteringExpressionText;
    }

    public void setClusteringExpressionText(String clusteringExpressionText) {
        this.clusteringExpressionText = clusteringExpressionText;
    }

    /** Records the validated WITH settings for the translator. */
    public void setResolvedOptions(int numClusters, String initMode, String metric, int dimension) {
        this.numClusters = numClusters;
        this.initMode = initMode;
        this.metric = metric;
        this.dimension = dimension;
    }

    public VariableExpr getClusterIdVar() {
        return clusterIdVar;
    }

    public void setClusterIdVar(VariableExpr clusterIdVar) {
        this.clusterIdVar = clusterIdVar;
    }

    public VariableExpr getCentroidVar() {
        return centroidVar;
    }

    public void setCentroidVar(VariableExpr centroidVar) {
        this.centroidVar = centroidVar;
    }

    public VariableExpr getRadiusVar() {
        return radiusVar;
    }

    public void setRadiusVar(VariableExpr radiusVar) {
        this.radiusVar = radiusVar;
    }

    public List<GbyVariableExpressionPair> getDecorPairList() {
        return decorPairList;
    }

    public void setDecorPairList(List<GbyVariableExpressionPair> decorPairList) {
        this.decorPairList = decorPairList;
    }

    public boolean hasDecorList() {
        return decorPairList != null && !decorPairList.isEmpty();
    }

    public boolean hasClusterMembersVar() {
        return clusterMembersVar != null;
    }

    public boolean hasClusterFieldList() {
        return clusterFieldList != null && !clusterFieldList.isEmpty();
    }

    public boolean hasWithOptions() {
        return withOptions != null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusteringExpr, clusterDescriptorVar, clusterMembersVar, clusterFieldList, withOptions);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ClusterbyClause)) {
            return false;
        }
        ClusterbyClause target = (ClusterbyClause) object;
        return Objects.equals(clusteringExpr, target.clusteringExpr)
                && Objects.equals(clusterDescriptorVar, target.clusterDescriptorVar)
                && Objects.equals(clusterMembersVar, target.clusterMembersVar)
                && Objects.equals(clusterFieldList, target.clusterFieldList)
                && Objects.equals(withOptions, target.withOptions);
    }
}
