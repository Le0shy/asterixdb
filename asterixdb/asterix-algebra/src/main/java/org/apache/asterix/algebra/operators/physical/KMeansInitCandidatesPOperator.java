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

import org.apache.asterix.runtime.operators.KMeansInitCandidatesOperatorDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansInitCandidatesOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Physical realization of {@link KMeansInitCandidatesOperator}: input 0 (the qualified vector stream)
 * keeps its child's partitioning; input 1 (the centroid pool) is REQUIRED BROADCAST, so the enforcer
 * inserts the broadcast exchange. Contributes the Store+Score Hyracks operator, which materializes the
 * partition once into a run file and emits the local top-l candidates.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY k-means|| init physical operator")
public class KMeansInitCandidatesPOperator extends AbstractPhysicalOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.KMEANS_INIT_CANDIDATES;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        pv[0] = StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR;
        pv[1] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(context.getComputationNodeDomain()),
                null);
        return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        // The output is partitioned like input 0 but carries ONLY the candidate variable: claiming the
        // child's delivered properties would advertise partitioning on variables this operator drops,
        // forcing the enforcer to insert bogus re-partitioning (e.g. hashing the candidate array).
        deliveredProperties = new StructuralPropertiesVector(
                new org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty(
                        context.getComputationNodeDomain()),
                null);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        KMeansInitCandidatesOperator kop = (KMeansInitCandidatesOperator) op;
        RecordDescriptor recDesc =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        // Each input branch delivers exactly ONE column by construction (the translator anchors and
        // projects a single variable). The variables recorded on the logical operator are plain fields —
        // invisible to variable-substitution rules — so they can drift through renames; resolve the column
        // positionally, using the (possibly stale) variable lookup only as a cross-check.
        int vectorColumn = resolveSingleColumn(inputSchemas[0], kop.getVectorVariable());
        int poolColumn = resolveSingleColumn(inputSchemas[1], kop.getPoolVariable());
        KMeansInitCandidatesOperatorDescriptor.Mode mode;
        switch (kop.getMode()) {
            case FINALIZE:
                mode = KMeansInitCandidatesOperatorDescriptor.Mode.FINALIZE;
                break;
            case WEIGH:
                mode = KMeansInitCandidatesOperatorDescriptor.Mode.WEIGH;
                break;
            case RECLUSTER:
                mode = KMeansInitCandidatesOperatorDescriptor.Mode.RECLUSTER;
                break;
            case LLOYD:
                mode = KMeansInitCandidatesOperatorDescriptor.Mode.LLOYD;
                break;
            case COST:
                mode = KMeansInitCandidatesOperatorDescriptor.Mode.COST;
                break;
            case SAMPLE:
                mode = KMeansInitCandidatesOperatorDescriptor.Mode.SAMPLE;
                break;
            case OVERSAMPLE_LOOP:
                mode = KMeansInitCandidatesOperatorDescriptor.Mode.OVERSAMPLE_LOOP;
                break;
            default:
                mode = KMeansInitCandidatesOperatorDescriptor.Mode.ROUND;
                break;
        }
        KMeansInitCandidatesOperatorDescriptor opDesc = new KMeansInitCandidatesOperatorDescriptor(builder.getJobSpec(),
                recDesc, mode, kop.getTopCount(), vectorColumn, poolColumn, kop.isPoolFromPriorRound(),
                kop.getSharedVectorsKey(), kop.isVectorsWriter(), kop.getSharedConsumerCount(), kop.getSeed(),
                kop.isKeepAllCandidates(), kop.getScoresKey(), kop.isScoresWriter(), kop.getLoopRounds());
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);
        ILogicalOperator src0 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src0, 0, op, 0);
        ILogicalOperator src1 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src1, 0, op, 1);
    }

    private static int resolveSingleColumn(IOperatorSchema schema,
            org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable var) throws AlgebricksException {
        int col = schema.findVariable(var);
        if (col >= 0) {
            return col;
        }
        if (schema.getSize() == 1) {
            return 0;
        }
        throw AlgebricksException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                "kmeans-init-candidates input schema", String.valueOf(schema.getSize()));
    }
}