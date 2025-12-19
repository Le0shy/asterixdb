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

package org.apache.hyracks.control.cc.scheduler;

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.job.JobRun;
import org.mockito.Mockito;

public class JobRunBuilder {
    private final JobRun run;

    public JobRunBuilder(long id) {
        this.run = Mockito.mock(JobRun.class, Mockito.RETURNS_DEEP_STUBS);
        when(run.getExceptions()).thenReturn(Collections.emptyList());
        when(run.getActivityClusterGraph().isReportTaskDetails()).thenReturn(true);
        when(run.getPendingExceptions()).thenReturn(Collections.emptyList());
        JobId jobId = new JobId(id);
        when(run.getJobId()).thenReturn(jobId);
        Set<String> nodes = new HashSet<>();
        nodes.add("node1");
        nodes.add("node2");
        when(run.getParticipatingNodeIds()).thenReturn(nodes);
        when(run.getCleanupPendingNodeIds()).thenReturn(nodes);
    }

    public JobRunBuilder withSchedulingType(JobTypeManager.JobSchedulingType jst) {
        when(run.getSchedulingType()).thenReturn(jst);
        return this;
    }

    public JobRunBuilder withExecutionTime(long executionTime) {
        when(run.getExecutionTime()).thenReturn(executionTime);
        return this;
    }

    public JobRunBuilder withPriority(int priority) {
        when(run.getPriority()).thenReturn(priority);
        return this;
    }

    public JobRunBuilder withJobSpecification(JobSpecification js) {
        when(run.getJobSpecification()).thenReturn(js);
        return this;
    }

    public JobRun build() {
        return this.run;
    }

}
