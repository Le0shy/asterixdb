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
