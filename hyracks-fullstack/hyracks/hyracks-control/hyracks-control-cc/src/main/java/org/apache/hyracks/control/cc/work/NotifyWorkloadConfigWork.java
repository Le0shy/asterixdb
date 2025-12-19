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

package org.apache.hyracks.control.cc.work;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.WorkloadManager;
import org.apache.hyracks.control.cc.scheduler.DeleteGroupInfo;
import org.apache.hyracks.control.cc.scheduler.EnableConfigInfo;
import org.apache.hyracks.control.cc.scheduler.IWorkloadConfigInfo;
import org.apache.hyracks.control.cc.scheduler.UpsertGroupInfo;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NotifyWorkloadConfigWork extends SynchronizableWork {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ClusterControllerService ccs;
    private IWorkloadConfigInfo workloadConfigInfo;

    public NotifyWorkloadConfigWork(ClusterControllerService ccs, IWorkloadConfigInfo workloadConfig) {
        this.ccs = ccs;
        this.workloadConfigInfo = workloadConfig;
    }

    @Override
    protected void doRun() throws Exception {
        WorkloadManager wlm = (WorkloadManager) ccs.getJobManager();
        switch (workloadConfigInfo.getType()) {
            case UPSERT_GROUP:
                UpsertGroupInfo upsertGroupInfo = (UpsertGroupInfo) workloadConfigInfo;
                wlm.addQueryGroups(upsertGroupInfo.getGroupToUpsert());
                break;
            case DELETE_GROUP:
                DeleteGroupInfo deleteGroupInfo = (DeleteGroupInfo) workloadConfigInfo;
                wlm.removeQueryGroups(deleteGroupInfo.getGroupToDelete());
                break;
            case ENABLE_CONFIG:
                wlm.enableSchedulerConfig((EnableConfigInfo) workloadConfigInfo);
                break;
            case SET_WORKLOAD_PARAMETERS:
                wlm.setWorkloadParameters((EnableConfigInfo) (workloadConfigInfo));
                break;
        }
    }
}
