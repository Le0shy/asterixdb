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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The current centroid set of one Lloyd loop partition, handed from the loop's tail back to its head.
 * <p>
 * Unlike the oversampling pool — which grows without bound and therefore lives in a run file — a Lloyd iteration
 * <em>replaces</em> its centroids, so the working set is bounded by the centroid count rather than by the data.
 * That makes an in-heap array adequate for the k values this feature targets, but not for arbitrary k: the set is
 * O(k · dim), so it is reached through this interface rather than as a bare array, leaving room for a
 * run-file-backed implementation without disturbing the operators. The dominant memory term in the loop is not
 * this store but the merge node's accumulator, which is O(partitions · k · dim).
 * <p>
 * Visibility between the writing task (Release) and the reading task (Controller) is supplied by the loop permit:
 * the writer stores before {@code release()} and the reader loads after {@code acquire()}, so the semaphore's
 * happens-before covers the handoff and no additional synchronization is required.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public interface CentroidStore {

    /** Replaces the current centroid set. Called once per iteration by the loop tail. */
    void put(List<double[]> centroids) throws HyracksDataException;

    /** The current centroid set, in centroid-index order. Never {@code null}; empty before the first put. */
    List<double[]> get() throws HyracksDataException;

    /** Releases any resources the implementation holds. Idempotent. */
    default void destroy() throws HyracksDataException {
    }

    /** The default implementation: the centroid set held in the task's heap. */
    final class InMemory implements CentroidStore {
        private volatile List<double[]> centroids = List.of();

        @Override
        public void put(List<double[]> next) {
            centroids = List.copyOf(next);
        }

        @Override
        public List<double[]> get() {
            return centroids;
        }
    }
}
