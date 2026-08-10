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
package org.apache.asterix.algebra.operators.physical;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * What the CLUSTER BY k-means|| stages share, whatever they compute: they are blocking, they emit only the
 * candidate variable, and each of their inputs delivers exactly one column. The stage a query actually runs is
 * chosen by {@code SetAsterixPhysicalOperatorsRule} from the logical operator's mode, one subclass per stage,
 * as the join and group-by families do -- so an input arity or a partitioning requirement is a property of the
 * class rather than a branch.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
public abstract class AbstractKMeansStagePOperator extends AbstractPhysicalOperator {

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        // The output is partitioned like input 0 but carries ONLY the candidate variable: claiming the
        // child's delivered properties would advertise partitioning on variables this operator drops,
        // forcing the enforcer to insert bogus re-partitioning (e.g. hashing the candidate array).
        deliveredProperties = new StructuralPropertiesVector(
                new RandomPartitioningProperty(context.getComputationNodeDomain()), null);
    }

    /**
     * Each input branch delivers exactly ONE column by construction: the translator anchors and projects a
     * single variable. The variables recorded on the logical operator are plain fields -- invisible to
     * variable-substitution rules -- so they can drift through renames; resolve the column positionally and
     * use the (possibly stale) variable lookup only as a cross-check.
     */
    protected static int resolveSingleColumn(IOperatorSchema schema, LogicalVariable var) throws AlgebricksException {
        int col = schema.findVariable(var);
        if (col >= 0) {
            return col;
        }
        if (schema.getSize() == 1) {
            return 0;
        }
        throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, "kmeans-stage input schema",
                String.valueOf(schema.getSize()));
    }
}
