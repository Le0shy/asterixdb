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

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.cluster.NodeManager;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.ipc.NodeControllerRemoteProxy;
import org.apache.hyracks.control.common.logs.LogFile;
import org.junit.Before;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;
import org.mockito.Mockito;

public class PriorityBasedQueueTest {
    private CCConfig ccConfig;

    @Before
    public void setup() throws IOException, CmdLineException {
        ccConfig = new CCConfig();
        ccConfig.getConfigManager().processConfig();
    }

    @Test
    public void test0() throws IOException, CmdLineException {
        IJobCapacityController jobCapacityController = mock(IJobCapacityController.class);
        when(jobCapacityController.getMaxAggregatedNumCores()).thenReturn(66);
        when(jobCapacityController.getMaxAggregatedMemoryByteSize()).thenReturn((long) 100);
        CapacityControllerGuard capacityControllerGuard = spy(new CapacityControllerGuard(jobCapacityController));
        IJobManager jobManager = spy(new JobManager(ccConfig, mockClusterControllerService(), jobCapacityController));
        PriorityBasedQueue priorityBasedQueue = spy(new PriorityBasedQueue(jobManager, capacityControllerGuard));

        List<JobRun> queuedJobs = new ArrayList<>();
        for (int id = 2; id <= 10; id += 2) {
            JobRun run = mockJobRunWithPriority(id / 2, id);
            JobSpecification job = mock(JobSpecification.class);
            IClusterCapacity requiredClusterCapacity = mockIClusterCapacity(4, 5);
            when(job.getRequiredClusterCapacity()).thenReturn(requiredClusterCapacity);
            when(run.getJobSpecification()).thenReturn(job);
            when(jobCapacityController.allocate(job, any(), anySet()))
                    .thenReturn(IJobCapacityController.JobSubmissionStatus.QUEUE);
            queuedJobs.add(run);
            priorityBasedQueue.add(run);
        }
        System.out.println(priorityBasedQueue.printQueueInfo());
    }

    @Test
    public void test1() throws IOException, CmdLineException {
        IJobCapacityController jobCapacityController = mock(IJobCapacityController.class);
        when(jobCapacityController.getMaxAggregatedNumCores()).thenReturn(66);
        when(jobCapacityController.getMaxAggregatedMemoryByteSize()).thenReturn((long) 100);
        CapacityControllerGuard capacityControllerGuard = spy(new CapacityControllerGuard(jobCapacityController));
        IJobManager jobManager = spy(new JobManager(ccConfig, mockClusterControllerService(), jobCapacityController));
        PriorityBasedQueue priorityBasedQueue = spy(new PriorityBasedQueue(jobManager, capacityControllerGuard));

        List<JobRun> queuedJobs = new ArrayList<>();
        for (int i = 1; i <= 10; i += 1) {
            for (int id = 2; id <= 10; id += 2) {
                JobRun run = mockJobRunWithPriority((i - 1) * 5 + id / 2, id);
                JobSpecification job = mock(JobSpecification.class);
                IClusterCapacity requiredClusterCapacity = mockIClusterCapacity(4, 5);
                when(job.getRequiredClusterCapacity()).thenReturn(requiredClusterCapacity);
                when(run.getJobSpecification()).thenReturn(job);
                when(capacityControllerGuard.allocate(run))
                        .thenReturn(IJobCapacityController.JobSubmissionStatus.EXECUTE);
                queuedJobs.add(run);
                priorityBasedQueue.add(run);
            }
        }
        System.out.println(priorityBasedQueue.printQueueInfo());
        int counter = 0;
        while (counter < 50) {
            List<JobRun> selected = priorityBasedQueue.pull();
            JobRun jobRun = selected.get(0);
            System.out.println("Selected Job #" + counter + ": " + jobRun.getJobId());
            priorityBasedQueue.notifyJobFinished(jobRun);
            capacityControllerGuard.release(jobRun);

            counter += 1;
            if (counter % 5 == 0) {
                System.out.println(priorityBasedQueue.printQueueInfo());
            }
        }
    }

    private JobRun mockJobRunWithPriority(long id, int priority) {
        JobRun run = mock(JobRun.class, Mockito.RETURNS_DEEP_STUBS);
        when(run.getExceptions()).thenReturn(Collections.emptyList());
        when(run.getActivityClusterGraph().isReportTaskDetails()).thenReturn(true);
        when(run.getPendingExceptions()).thenReturn(Collections.emptyList());
        JobId jobId = new JobId(id);
        when(run.getJobId()).thenReturn(jobId);
        when(run.getSchedulingType()).thenReturn(JobTypeManager.JobSchedulingType.NORMAL);
        when(run.getPriority()).thenReturn(priority);

        Set<String> nodes = new HashSet<>();
        nodes.add("node1");
        nodes.add("node2");
        when(run.getParticipatingNodeIds()).thenReturn(nodes);
        when(run.getCleanupPendingNodeIds()).thenReturn(nodes);
        return run;
    }

    private IClusterCapacity mockIClusterCapacity(int cores, long mems) {
        IClusterCapacity clusterCapacity = mock(IClusterCapacity.class);
        when(clusterCapacity.getAggregatedCores()).thenReturn(cores);
        when(clusterCapacity.getAggregatedMemoryByteSize()).thenReturn((mems));
        return clusterCapacity;
    }

    private ClusterControllerService mockClusterControllerService() {
        ClusterControllerService ccs = mock(ClusterControllerService.class);
        CCServiceContext ccServiceCtx = mock(CCServiceContext.class);
        LogFile logFile = mock(LogFile.class);
        INodeManager nodeManager = mockNodeManager();
        when(ccs.getContext()).thenReturn(ccServiceCtx);
        when(ccs.getJobLogFile()).thenReturn(logFile);
        when(ccs.getNodeManager()).thenReturn(nodeManager);
        when(ccs.getCCConfig()).thenReturn(ccConfig);
        return ccs;
    }

    private INodeManager mockNodeManager() {
        INodeManager nodeManager = mock(NodeManager.class);
        NodeControllerState ncState = mock(NodeControllerState.class);
        NodeControllerRemoteProxy nodeController = mock(NodeControllerRemoteProxy.class);
        when(nodeManager.getNodeControllerState(any())).thenReturn(ncState);
        when(ncState.getNodeController()).thenReturn(nodeController);
        return nodeManager;
    }
}
