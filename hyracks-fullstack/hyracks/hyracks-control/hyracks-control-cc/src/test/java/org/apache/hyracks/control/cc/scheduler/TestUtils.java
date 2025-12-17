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

import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.cluster.NodeManager;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.WorkloadManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.ipc.NodeControllerRemoteProxy;
import org.apache.hyracks.control.common.logs.LogFile;

public class TestUtils {

    public static CapacityControllerGuard mockCapacityControllerGuard(IJobCapacityController jobCapacityController) {
        return spy(new CapacityControllerGuard(jobCapacityController));
    }

    public static IJobManager mockJobManager(IJobCapacityController jobCapacityController, CCConfig ccConfig) {
        return spy(new WorkloadManager(ccConfig, mockClusterControllerService(ccConfig), jobCapacityController));
    }

    public static IJobCapacityController mockJobCapacityController(int cores, long mem) {
        IJobCapacityController jobCapacityController = mock(IJobCapacityController.class);
        when(jobCapacityController.getMaxAggregatedNumCores()).thenReturn(cores);
        when(jobCapacityController.getMaxAggregatedMemoryByteSize()).thenReturn(mem);
        return jobCapacityController;
    }

    public static JobSpecification mockJobSpecification(int cores, long mem) {
        JobSpecification js = mock(JobSpecification.class);
        IClusterCapacity requiredClusterCapacity = mockIClusterCapacity(cores, mem);
        when(js.getRequiredClusterCapacity()).thenReturn(requiredClusterCapacity);
        return js;
    }

    private static IClusterCapacity mockIClusterCapacity(int cores, long mem) {
        IClusterCapacity clusterCapacity = mock(IClusterCapacity.class);
        when(clusterCapacity.getAggregatedCores()).thenReturn(cores);
        when(clusterCapacity.getAggregatedMemoryByteSize()).thenReturn((mem));
        return clusterCapacity;
    }

    private static ClusterControllerService mockClusterControllerService(CCConfig ccConfig) {
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

    private static INodeManager mockNodeManager() {
        INodeManager nodeManager = mock(NodeManager.class);
        NodeControllerState ncState = mock(NodeControllerState.class);
        NodeControllerRemoteProxy nodeController = mock(NodeControllerRemoteProxy.class);
        when(nodeManager.getNodeControllerState(any())).thenReturn(ncState);
        when(ncState.getNodeController()).thenReturn(nodeController);
        return nodeManager;
    }

}
