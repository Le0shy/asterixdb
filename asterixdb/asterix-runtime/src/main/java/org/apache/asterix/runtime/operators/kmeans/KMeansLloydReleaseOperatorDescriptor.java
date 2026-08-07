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
package org.apache.asterix.runtime.operators.kmeans;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ Lloyd loop — the tail that closes the loop back: publish the iteration's centroids to this
 * partition's store, then wake its Controller.
 * <p>
 * This is the counterpart of the oversampling loop's release, and differs in one respect: an oversampling round
 * <em>appends</em> its draws to a growing pool, whereas a Lloyd iteration <em>replaces</em> the centroid set
 * outright, so the centroids are buffered until the end marker and then published in one shot. Publishing before
 * releasing the permit is what makes the new set visible to the Controller's next read — the semaphore supplies
 * the happens-before, so there is no lock and no volatile handshake beyond the store's own field.
 * <p>
 * The operator has no output. Its effect is entirely the store update plus the permit, which is why the physical
 * operator registers it as a job root — nothing downstream would otherwise pull its branch into the schedule.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY Lloyd loop: centroid publish + loop-back release (Op3)")
public class KMeansLloydReleaseOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final String loopKey;

    public KMeansLloydReleaseOperatorDescriptor(IOperatorDescriptorRegistry spec, String loopKey) {
        super(spec, 1, 0);
        this.loopKey = loopKey;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
            private final FrameTupleReference tuple = new FrameTupleReference();
            private final List<double[]> pending = new ArrayList<>();
            private Semaphore permit;
            private CentroidStore store;

            @Override
            public void open() throws HyracksDataException {
                try {
                    LoopControlState ctl = (LoopControlState) LoopControlState.await(ctx::getStateObject,
                            LoopControlState.controlStateId(loopKey, partition));
                    permit = ctl.getPermit();
                    store = ctl.getCentroids();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tuple.reset(accessor, i);
                    int kind = IntegerPointable.getInteger(tuple.getFieldData(3), tuple.getFieldStart(3));
                    if (kind == KMeansLoopIO.KIND_END) {
                        // The whole set for this iteration has arrived: publish it, THEN wake the Controller.
                        store.put(pending);
                        pending.clear();
                        permit.release();
                    } else {
                        pending.add(KMeansLoopIO.readRawVector(tuple.getFieldData(4), tuple.getFieldStart(4),
                                tuple.getFieldLength(4)));
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
            }
        };
    }
}
