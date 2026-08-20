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
package org.apache.asterix.lang.sqlpp.expression;

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The clustering a CLUSTER BY block asks for: a stream of vectors, and how to divide it.
 * <p>
 * The rewrite that desugars the clause produces one of these where the centroids are needed, and the
 * translator turns it into a ClusterByOperator. It is the whole of what the clause asks the plan for --
 * everything else the clause implies (labelling rows, grouping them, the cluster descriptor) is ordinary
 * SQL++ the rewrite writes around it.
 * <p>
 * Only {@code vectors} is an expression, so it is the only part later rewrites descend into. The rest comes
 * from the WITH clause, is constant by the time the clause is parsed, and is held as itself rather than as
 * literal arguments that would have to be read back out positionally.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY as an AST node, replacing the internal cluster-by marker function")
public class ClusterByExpr extends AbstractExpression {

    private Expression vectors;
    private final int numClusters;
    private final String initMode;
    private final String metric;

    public ClusterByExpr(Expression vectors, int numClusters, String initMode, String metric) {
        this.vectors = Objects.requireNonNull(vectors);
        this.numClusters = numClusters;
        this.initMode = Objects.requireNonNull(initMode);
        this.metric = Objects.requireNonNull(metric);
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public Kind getKind() {
        return Kind.CLUSTER_BY_EXPRESSION;
    }

    public Expression getVectors() {
        return vectors;
    }

    public void setVectors(Expression vectors) {
        this.vectors = Objects.requireNonNull(vectors);
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

    @Override
    public String toString() {
        return "CLUSTER BY " + vectors + " INTO " + numClusters + " (" + initMode + ", " + metric + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(vectors, numClusters, initMode, metric);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ClusterByExpr)) {
            return false;
        }
        ClusterByExpr target = (ClusterByExpr) object;
        return numClusters == target.numClusters && Objects.equals(vectors, target.vectors)
                && Objects.equals(initMode, target.initMode) && Objects.equals(metric, target.metric);
    }
}
