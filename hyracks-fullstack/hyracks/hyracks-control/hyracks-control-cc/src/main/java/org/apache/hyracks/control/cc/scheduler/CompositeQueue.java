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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.util.annotations.GuardedBy;
import org.apache.hyracks.util.annotations.NotThreadSafe;

@NotThreadSafe
@GuardedBy("JobManager")
public class CompositeQueue implements IJobQueue {
    private final CapacityControllerGuard capacityControllerGuard;
    //private final IJobCapacityController jobCapacityController;
    private final IJobQueue SQAJobQueue;
    private final IJobQueue priorityBasedQueue;

    public CompositeQueue(IJobManager jobManager, CapacityControllerGuard capacityControllerGuard) {
        SQAJobQueue = new DedicatedJobQueue(jobManager, capacityControllerGuard);
        priorityBasedQueue = new PriorityBasedQueue(jobManager, capacityControllerGuard);
        this.capacityControllerGuard = capacityControllerGuard;
    }

    @Override
    public void add(JobRun run) throws HyracksException {
        JobTypeManager.JobSchedulingType schedulingType = run.getSchedulingType();
        if (schedulingType == JobTypeManager.JobSchedulingType.SHORT) {
            SQAJobQueue.add(run);
        } else {
            priorityBasedQueue.add(run);
        }
    }

    @Override
    public JobRun remove(JobId jobId) {
        JobRun removedJob;
        removedJob = SQAJobQueue.remove(jobId);
        if (removedJob != null) {
            return removedJob;
        }
        return priorityBasedQueue.remove(jobId);
    }

    @Override
    public JobRun get(JobId jobId) {
        JobRun acquiredJob;
        acquiredJob = SQAJobQueue.get(jobId);
        if (acquiredJob != null) {
            return acquiredJob;
        }
        return priorityBasedQueue.get(jobId);
    }

    @Override
    public List<JobRun> pull() {
        List<JobRun> jobRuns;
        if (capacityControllerGuard.isSQAResourcesAvailable()) {
            jobRuns = SQAJobQueue.pull();
        } else {
            jobRuns = new ArrayList<>();
        }
        jobRuns.addAll(priorityBasedQueue.pull());
        return jobRuns;
    }

    @Override
    public Collection<JobRun> jobs() {
        List<JobRun> jobs = new ArrayList<>();
        jobs.addAll(SQAJobQueue.jobs());
        jobs.addAll(priorityBasedQueue.jobs());
        return Collections.unmodifiableCollection(jobs);
    }

    @Override
    public void clear() {

    }

    @Override
    public int size() {
        return SQAJobQueue.size() + priorityBasedQueue.size();
    }

    @Override
    public void notifyJobFinished(JobRun run) {
        JobTypeManager.JobSchedulingType schedulingType = run.getSchedulingType();
        if (schedulingType == JobTypeManager.JobSchedulingType.SHORT) {
            SQAJobQueue.notifyJobFinished(run);
        } else {
            priorityBasedQueue.notifyJobFinished(run);
        }
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
