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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

/**
 * CLUSTER BY k-means‖ initialization loop: the per-partition loop-back rendezvous shared, via
 * <b>joblet-scoped state</b>, by the co-located Cost (Op1), Sample (Op3) and Release (Op5) tasks of one
 * {@code OVERSAMPLE_LOOP} sub-graph on one NC. Because the joblet state store (see {@code Joblet.stateObjectMap})
 * spans all of a job's operators on an NC and is keyed by {@link #getId()}, Op1 creates one of these under a
 * per-partition token and Op5/Op3 retrieve it by that same token.
 * <p>
 * It carries only the {@link Semaphore} permit that paces the loop: Op1 {@code acquire()}s after emitting each
 * round's local potential; Op5 {@code release()}s after appending that round's global draws to the shared pool
 * run file. The {@code release()}→{@code acquire()} pair supplies the happens-before that makes the pool run
 * file's freshly appended size visible to Op1's next-round read. The growing pool and the resident vectors live
 * in their own {@code MaterializerTaskState} run files (also joblet-scoped, keyed per partition); this object is
 * just the synchronization handle.
 * <p>
 * A reader (Op3/Op5) may look this up before Op1 has created it (the pipeline starts all tasks at once), so
 * callers must guard the lookup with a short wait, as the prototype does; the data-flow ordering (Op3/Op5 touch
 * the loop only after Op1's first cost) guarantees it is present by first use.
 */
@org.apache.hyracks.util.annotations.AiProvenance(agent = org.apache.hyracks.util.annotations.AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = org.apache.hyracks.util.annotations.AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY k-means|| init loop: joblet-shared permit holder")
public final class LoopControlState extends AbstractStateObject {

    // Not serialized: this state never leaves the NC (joblet-local). The semaphore is created empty; Op5 grants
    // one permit per completed round.
    private final transient Semaphore permit = new Semaphore(0);

    public LoopControlState(JobId jobId, Object id) {
        super(jobId, id);
    }

    public Semaphore getPermit() {
        return permit;
    }

    /** The joblet-state id under which Op1 registers, and Op3/Op5 look up, this partition's control state. */
    public static Object controlStateId(String loopKey, int partition) {
        return loopKey + "#loopctl#" + partition;
    }

    /** The joblet-state id of this partition's shared pool run file ({@code MaterializerTaskState}). */
    public static Object poolStateId(String loopKey, int partition) {
        return loopKey + "#pool#" + partition;
    }

    /** The joblet-state id of this partition's shared resident-vector run file ({@code MaterializerTaskState}). */
    public static Object vectorsStateId(String loopKey, int partition) {
        return loopKey + "#vec#" + partition;
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
        // Never serialized; joblet-local.
    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
        // Never serialized; joblet-local.
    }

    /** Convenience: look up (with a bounded wait) a joblet state object a sibling operator registered. */
    public static IStateObject await(java.util.function.Function<Object, IStateObject> lookup, Object id)
            throws InterruptedException {
        for (int i = 0; i < 120000; i++) {
            IStateObject s = lookup.apply(id);
            if (s != null) {
                return s;
            }
            Thread.sleep(1);
        }
        throw new IllegalStateException("joblet state never appeared: " + id);
    }
}
