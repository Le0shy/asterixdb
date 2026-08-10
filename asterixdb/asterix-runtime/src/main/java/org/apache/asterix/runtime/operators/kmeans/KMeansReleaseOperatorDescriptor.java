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
import java.util.concurrent.Semaphore;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ initialization loop — <b>Op5 Release</b>: the tail that closes the loop-back.
 * One partition per co-located group (shares an NC, permit, and pool run file with the Cost/Sample operators). It
 * consumes each round's global draw union + end marker (broadcast from PoolMerge, Op4) and:
 * <ul>
 * <li>for each drawn vector, <b>appends</b> it to this partition's shared pool run file (turning {@code pool[r]}
 * into {@code pool[r+1]}) — the raw-double {@link KMeansLoopIO#POOL_RD} format that Cost/Sample read back;</li>
 * <li>on the end marker, having appended all of the round's draws, {@link Semaphore#release()}s the permit —
 * waking the co-located Cost (Op1), which was parked on {@code acquire()}, for the next round.</li>
 * </ul>
 * Appending strictly before releasing is what guarantees Cost reads a complete {@code pool[r+1]}; the
 * {@code release()}/{@code acquire()} pair also supplies the happens-before for the appended frames' visibility.
 * The pool run file and permit are looked up from joblet state (created by Cost) with a bounded wait, since the
 * pipeline starts all tasks at once. Op5 has no output (a sink); the loop ends when its input EOFs (Cost closes).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
public class KMeansReleaseOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final String loopKey;

    public KMeansReleaseOperatorDescriptor(IOperatorDescriptorRegistry spec, String loopKey) {
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
            private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
            private Semaphore permit;
            private MaterializerTaskState pool;
            private VSizeFrame poolFrame;
            private FrameTupleAppender poolAppender;

            @Override
            public void open() throws HyracksDataException {
                try {
                    LoopControlState ctl = (LoopControlState) LoopControlState.await(ctx::getStateObject,
                            LoopControlState.controlStateId(loopKey, partition));
                    permit = ctl.getPermit();
                    pool = (MaterializerTaskState) LoopControlState.await(ctx::getStateObject,
                            LoopControlState.poolStateId(loopKey, partition));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
                poolFrame = new VSizeFrame(ctx);
                poolAppender = new FrameTupleAppender(poolFrame);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tuple.reset(accessor, i);
                    int kind = IntegerPointable.getInteger(tuple.getFieldData(3), tuple.getFieldStart(3));
                    if (kind == KMeansLoopIO.KIND_END) {
                        flushPool(); // all of this round's draws are now in the pool run file ...
                        permit.release(); // ... so it is safe to wake Cost for the next round
                    } else {
                        appendDrawToPool(tuple);
                    }
                }
            }

            // Append one drawn vector to the shared pool run file, in POOL_RD (raw double[]) form.
            private void appendDrawToPool(FrameTupleReference t) throws HyracksDataException {
                double[] vec = KMeansLoopIO.readRawVector(t.getFieldData(4), t.getFieldStart(4), t.getFieldLength(4));
                tb.reset();
                KMeansLoopIO.writeRawVector(tb, vec);
                if (!poolAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    flushPool();
                    if (!poolAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        throw HyracksDataException.create(new IllegalStateException("pool vector exceeds a frame"));
                    }
                }
            }

            private void flushPool() throws HyracksDataException {
                if (poolAppender.getTupleCount() > 0) {
                    pool.appendFrame(poolAppender.getBuffer());
                    poolAppender.reset(poolFrame, true);
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
