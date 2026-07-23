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

import java.util.Arrays;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.runtime.operators.KMeansInitCandidatesOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansCostControllerOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansLoopIO;
import org.apache.asterix.runtime.operators.kmeans.KMeansPhiMergeOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansPoolMergeOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansReleaseOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansSampleOperatorDescriptor;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
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
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.MToNBroadcastConnectorDescriptor;
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
        ILogicalOperator src0 = op.getInputs().get(0).getValue();
        ILogicalOperator src1 = op.getInputs().get(1).getValue();
        // Route B: an OVERSAMPLE_LOOP spanning more than one NC cannot use Route A's in-JVM barrier, so it is
        // realized as the systolic 5-operator sub-graph injected here. A single-NC OVERSAMPLE_LOOP keeps the
        // leaner barrier operator (the default path below); every other mode is always the default path.
        if (kop.getMode() == KMeansInitCandidatesOperator.Mode.OVERSAMPLE_LOOP) {
            String[] clusterLocations =
                    ((MetadataProvider) context.getMetadataProvider()).getClusterLocations().getLocations();
            int nNC = (int) Arrays.stream(clusterLocations).distinct().count();
            if (nNC > 1) {
                contributeSystolicLoop(builder, kop, (AbstractLogicalOperator) op, recDesc, vectorColumn, poolColumn,
                        clusterLocations, src0, src1);
                return;
            }
        }
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
        builder.contributeGraphEdge(src0, 0, op, 0);
        builder.contributeGraphEdge(src1, 0, op, 1);
    }

    /**
     * Route B (multi-NC): inject the systolic 5-operator sub-graph for one {@code OVERSAMPLE_LOOP} onto the job
     * spec. Op1 (Cost/Controller) is the registered descriptor, so the builder wires the vectors (input 0) and
     * seed (input 1) into it and the parent WEIGH reads the final pool from its output 0; the per-round potential
     * (output 1) and Op2..Op5 are internal edges wired here with pipelined broadcast connectors. Op1/Op3/Op5 are
     * pinned to the SAME cluster locations so partition i of each co-locates on one NC (sharing that NC's permit +
     * pool/vector run files via joblet state); the PhiMerge/PoolMerge nodes are single-partition. Op5 is a sink
     * dead-end, so it is registered as a job root to ensure its branch is scheduled.
     */
    private void contributeSystolicLoop(IHyracksJobBuilder builder, KMeansInitCandidatesOperator kop,
            AbstractLogicalOperator op, RecordDescriptor poolEnvelopeRecDesc, int vectorColumn, int seedColumn,
            String[] clusterLocations, ILogicalOperator src0, ILogicalOperator src1) throws AlgebricksException {
        JobSpecification spec = builder.getJobSpec();
        // Unique + stable per loop instance (one per query); baked into all five descriptors so every partition
        // and NC agrees on the joblet-state keys.
        String loopKey = "kmeansSystolicLoop#" + kop.getCandidateVariable();
        int participants = clusterLocations.length;

        // Op1 Cost/Controller — the registered descriptor (inputs land here; WEIGH reads output 0 = the pool).
        KMeansCostControllerOperatorDescriptor op1 = new KMeansCostControllerOperatorDescriptor(spec,
                poolEnvelopeRecDesc, KMeansLoopIO.SCALAR_RD, loopKey, vectorColumn, seedColumn, kop.getLoopRounds());
        contributeOpDesc(builder, op, op1);
        builder.contributeGraphEdge(src0, 0, op, 0);
        builder.contributeGraphEdge(src1, 0, op, 1);

        // The internal loop operators.
        KMeansPhiMergeOperatorDescriptor op2 =
                new KMeansPhiMergeOperatorDescriptor(spec, KMeansLoopIO.SCALAR_RD, participants);
        KMeansSampleOperatorDescriptor op3 = new KMeansSampleOperatorDescriptor(spec, KMeansLoopIO.DRAW_RD, loopKey,
                kop.getTopCount(), kop.getSeed());
        KMeansPoolMergeOperatorDescriptor op4 =
                new KMeansPoolMergeOperatorDescriptor(spec, KMeansLoopIO.DRAW_RD, participants);
        KMeansReleaseOperatorDescriptor op5 = new KMeansReleaseOperatorDescriptor(spec, loopKey);

        // Partition constraints (registered via the builder so the job-gen finalizer does not double-assign a
        // default). Op1/Op3/Op5 share identical absolute locations -> co-located per partition; merges single-node.
        AlgebricksAbsolutePartitionConstraint coLocated = new AlgebricksAbsolutePartitionConstraint(clusterLocations);
        builder.contributeAlgebricksPartitionConstraint(op1, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op3, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op5, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op2, new AlgebricksCountPartitionConstraint(1));
        builder.contributeAlgebricksPartitionConstraint(op4, new AlgebricksCountPartitionConstraint(1));

        // Internal pipelined broadcast edges: Op1.localSigma -> PhiMerge -> Sample -> PoolMerge -> Release.
        // (Broadcast into a single-partition merge is a CONCURRENT M-to-1; never the sequential merging connector,
        // which would deadlock against the permit-paced producers.)
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op1, 1, op2, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op2, 0, op3, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op3, 0, op4, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op4, 0, op5, 0);

        // Release is a sink dead-end (its "output" is side-effects: pool append + permit release), not upstream of
        // the main result, so register it as a root or its branch would not be scheduled.
        spec.addRoot(op5);
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