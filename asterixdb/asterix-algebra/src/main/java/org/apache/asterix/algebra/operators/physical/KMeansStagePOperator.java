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

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.runtime.operators.KMeansMergeOperatorDescriptor;
import org.apache.asterix.runtime.operators.KMeansWeighOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansCentroidMergeOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansCostControllerOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansLloydControllerOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansLloydReleaseOperatorDescriptor;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansStageOperator;
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
 * Physical realization of {@link KMeansStageOperator}, dispatched by mode: WEIGH contributes a
 * two-input {@link KMeansWeighOperatorDescriptor} (partitioned vectors at input 0, broadcast pool at input 1);
 * RECLUSTER/LLOYD contribute a single-input {@link KMeansMergeOperatorDescriptor} whose sole input is the
 * broadcast partials; OVERSAMPLE_LOOP is injected as the systolic 5-operator sub-graph. The pool input is
 * always REQUIRED BROADCAST, so the enforcer inserts the broadcast exchange.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY k-means|| init physical operator")
public class KMeansStagePOperator extends AbstractPhysicalOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.KMEANS_STAGE;
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
        KMeansStageOperator kop = (KMeansStageOperator) op;
        StructuralPropertiesVector broadcastPool = new StructuralPropertiesVector(
                new BroadcastPartitioningProperty(context.getComputationNodeDomain()), null);
        StructuralPropertiesVector[] pv;
        if (kop.getMode() == KMeansStageOperator.Mode.RECLUSTER || kop.getMode() == KMeansStageOperator.Mode.LLOYD) {
            // Single-input merge: the sole input IS the pool/partials and must be broadcast so every partition
            // reduces the complete partial set (an un-broadcast input would reduce only local partials).
            pv = new StructuralPropertiesVector[] { broadcastPool };
        } else {
            // Two inputs: the partitioned vectors (input 0, no requirement) and the broadcast pool (input 1).
            pv = new StructuralPropertiesVector[] { StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR, broadcastPool };
        }
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
        KMeansStageOperator kop = (KMeansStageOperator) op;
        RecordDescriptor recDesc =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        // Each input branch delivers exactly ONE column by construction (the translator anchors and
        // projects a single variable). The variables recorded on the logical operator are plain fields —
        // invisible to variable-substitution rules — so they can drift through renames; resolve the column
        // positionally, using the (possibly stale) variable lookup only as a cross-check.
        //
        // RECLUSTER/LLOYD are single-input MERGES: they read ONLY the broadcast partials, so their sole input
        // (index 0) IS the pool. WEIGH and OVERSAMPLE_LOOP are two-input (vectors at 0, pool at 1).
        if (kop.getMode() == KMeansStageOperator.Mode.RECLUSTER || kop.getMode() == KMeansStageOperator.Mode.LLOYD) {
            int poolColumn = resolveSingleColumn(inputSchemas[0], kop.getPoolVariable());
            KMeansMergeOperatorDescriptor.Mode mergeMode = kop.getMode() == KMeansStageOperator.Mode.RECLUSTER
                    ? KMeansMergeOperatorDescriptor.Mode.RECLUSTER : KMeansMergeOperatorDescriptor.Mode.LLOYD;
            KMeansMergeOperatorDescriptor mergeDesc = new KMeansMergeOperatorDescriptor(builder.getJobSpec(), recDesc,
                    mergeMode, kop.getTopCount(), poolColumn);
            contributeOpDesc(builder, (AbstractLogicalOperator) op, mergeDesc);
            builder.contributeGraphEdge(op.getInputs().get(0).getValue(), 0, op, 0);
            return;
        }
        int vectorColumn = resolveSingleColumn(inputSchemas[0], kop.getVectorVariable());
        int poolColumn = resolveSingleColumn(inputSchemas[1], kop.getPoolVariable());
        ILogicalOperator src0 = op.getInputs().get(0).getValue();
        ILogicalOperator src1 = op.getInputs().get(1).getValue();
        // OVERSAMPLE_LOOP is ALWAYS realized as the systolic 5-operator sub-graph injected here, on any topology
        // (the in-JVM-barrier single-NC fallback has been retired — one code path). On a single NC all partitions
        // simply co-locate there; the merges are single-node; the connectors are intra-JVM.
        if (kop.getMode() == KMeansStageOperator.Mode.OVERSAMPLE_LOOP) {
            String[] clusterLocations =
                    ((MetadataProvider) context.getMetadataProvider()).getClusterLocations().getLocations();
            contributeSystolicLoop(builder, kop, (AbstractLogicalOperator) op, recDesc, vectorColumn, poolColumn,
                    clusterLocations, src0, src1);
            return;
        }
        if (kop.getMode() == KMeansStageOperator.Mode.LLOYD_LOOP) {
            String[] clusterLocations =
                    ((MetadataProvider) context.getMetadataProvider()).getClusterLocations().getLocations();
            contributeLloydLoop(builder, kop, (AbstractLogicalOperator) op, recDesc, vectorColumn, poolColumn,
                    clusterLocations, src0, src1);
            return;
        }
        if (kop.getMode() != KMeansStageOperator.Mode.WEIGH) {
            throw new IllegalStateException("unexpected KMeansStage mode: " + kop.getMode());
        }
        KMeansWeighOperatorDescriptor weighDesc = new KMeansWeighOperatorDescriptor(builder.getJobSpec(), recDesc,
                kop.getTopCount(), vectorColumn, poolColumn, kop.isPoolFromPriorRound(), kop.getSharedVectorsKey(),
                kop.isVectorsWriter(), kop.getSharedConsumerCount());
        contributeOpDesc(builder, (AbstractLogicalOperator) op, weighDesc);
        builder.contributeGraphEdge(src0, 0, op, 0);
        builder.contributeGraphEdge(src1, 0, op, 1);
    }

    /**
     * Inject the systolic 5-operator sub-graph for one {@code OVERSAMPLE_LOOP} onto the job
     * spec. Op1 (Cost/Controller) is the registered descriptor, so the builder wires the vectors (input 0) and
     * seed (input 1) into it and the parent WEIGH reads the final pool from its output 0; the per-round potential
     * (output 1) and Op2..Op5 are internal edges wired here with pipelined broadcast connectors. Op1/Op3/Op5 are
     * pinned to the SAME cluster locations so partition i of each co-locates on one NC (sharing that NC's permit +
     * pool/vector run files via joblet state); the PhiMerge/PoolMerge nodes are single-partition. Op5 is a sink
     * dead-end, so it is registered as a job root to ensure its branch is scheduled.
     */
    private void contributeSystolicLoop(IHyracksJobBuilder builder, KMeansStageOperator kop, AbstractLogicalOperator op,
            RecordDescriptor poolEnvelopeRecDesc, int vectorColumn, int seedColumn, String[] clusterLocations,
            ILogicalOperator src0, ILogicalOperator src1) throws AlgebricksException {
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

    /**
     * Inject the systolic 3-operator sub-graph for one {@code LLOYD_LOOP} onto the job spec — the same shape as
     * the oversampling loop, one operator shorter because a Lloyd iteration has a single reduce (the centroid
     * means) where an oversampling round has two (the potential, then the draws).
     * <p>
     * Op1 (Controller) is the registered descriptor, so the builder wires the vectors (input 0) and the initial
     * centroids (input 1) into it, and whatever consumes this node reads the final centroid set from its output
     * 0; the per-iteration partials (output 1) and Op2/Op3 are internal edges wired here. Op1/Op3 are pinned to
     * the SAME cluster locations so partition i of each co-locates on one node and shares that node's permit and
     * centroid store; the merge is single-partition. Op3 is a sink dead-end, so it is registered as a job root or
     * its branch would never be scheduled.
     */
    private void contributeLloydLoop(IHyracksJobBuilder builder, KMeansStageOperator kop, AbstractLogicalOperator op,
            RecordDescriptor centroidRecDesc, int vectorColumn, int centroidColumn, String[] clusterLocations,
            ILogicalOperator src0, ILogicalOperator src1) throws AlgebricksException {
        JobSpecification spec = builder.getJobSpec();
        // Unique + stable per loop instance, and distinct from any oversampling loop's key in the same job.
        String loopKey = "kmeansLloydLoop#" + kop.getCandidateVariable();
        int participants = clusterLocations.length;

        KMeansLloydControllerOperatorDescriptor op1 = new KMeansLloydControllerOperatorDescriptor(spec, centroidRecDesc,
                KMeansLoopIO.PARTIAL_RD, loopKey, vectorColumn, centroidColumn, kop.getLoopRounds());
        contributeOpDesc(builder, op, op1);
        builder.contributeGraphEdge(src0, 0, op, 0);
        builder.contributeGraphEdge(src1, 0, op, 1);

        KMeansCentroidMergeOperatorDescriptor op2 =
                new KMeansCentroidMergeOperatorDescriptor(spec, KMeansLoopIO.DRAW_RD, participants);
        KMeansLloydReleaseOperatorDescriptor op3 = new KMeansLloydReleaseOperatorDescriptor(spec, loopKey);

        AlgebricksAbsolutePartitionConstraint coLocated = new AlgebricksAbsolutePartitionConstraint(clusterLocations);
        builder.contributeAlgebricksPartitionConstraint(op1, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op3, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op2, new AlgebricksCountPartitionConstraint(1));

        // Internal pipelined broadcast edges: Op1.partials -> CentroidMerge -> Release.
        // (Broadcast into a single-partition merge is a CONCURRENT M-to-1; never the sequential merging
        // connector, which would deadlock against the permit-paced producers.)
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op1, 1, op2, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op2, 0, op3, 0);

        spec.addRoot(op3);
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
                "kmeans-stage input schema", String.valueOf(schema.getSize()));
    }
}
