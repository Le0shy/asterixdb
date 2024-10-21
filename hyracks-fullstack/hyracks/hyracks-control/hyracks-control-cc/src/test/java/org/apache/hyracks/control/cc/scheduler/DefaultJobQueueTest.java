package org.apache.hyracks.control.cc.scheduler;

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
import org.apache.hyracks.ipc.sockets.SslHandshake;
import org.junit.Test;
import org.junit.Before;
import org.kohsuke.args4j.CmdLineException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class DefaultJobQueueTest {
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
        when(jobCapacityController.getMaxAggregatedMemoryByteSize()).thenReturn((long)100);

        CapacityControllerGuard capacityControllerGuard = spy(new CapacityControllerGuard(jobCapacityController));
        IJobManager jobManager = spy(new JobManager(ccConfig, mockClusterControllerService(), jobCapacityController));
        DefaultJobQueue defaultJobQueue = spy(new DefaultJobQueue(jobManager, capacityControllerGuard));

        // Submits runnable jobs.
        // add ten jobs based on memory ratio
        List<JobRun> queuedJobs = new ArrayList<>();
        for (int id = 1; id <= 10; id += 1) {
            JobRun run = mockDefaultJobRun(id);
            JobSpecification job = mock(JobSpecification.class);
            IClusterCapacity requiredClusterCapacity = mockIClusterCapacity(4, id * 5);
            when(job.getRequiredClusterCapacity()).thenReturn(requiredClusterCapacity);
            when(run.getJobSpecification()).thenReturn(job);
            when(jobCapacityController.allocate(job)).thenReturn(IJobCapacityController.JobSubmissionStatus.QUEUE);
            queuedJobs.add(run);
            defaultJobQueue.add(run);
        }

        System.out.println(defaultJobQueue.printQueueInfo());
    }

    @Test
    public void test1() throws IOException, CmdLineException {
        IJobCapacityController jobCapacityController = mock(IJobCapacityController.class);
        when(jobCapacityController.getMaxAggregatedNumCores()).thenReturn(66);
        when(jobCapacityController.getMaxAggregatedMemoryByteSize()).thenReturn((long) 100);
        CapacityControllerGuard capacityControllerGuard = spy(new CapacityControllerGuard(jobCapacityController));
        IJobManager jobManager = spy(new JobManager(ccConfig, mockClusterControllerService(), jobCapacityController));
        DefaultJobQueue defaultJobQueue = spy(new DefaultJobQueue(jobManager, capacityControllerGuard));

        long time = 0;
        when(defaultJobQueue.getCurrentTime()).thenReturn((long)time);
        List<JobRun> queuedJobs = new ArrayList<>();

        for (int i = 1; i <= 10; i += 1) {
            for (int j = 1; j <= 10; j += 1) {
                JobRun run = mockJobRunWithExecutionTime((i - 1) * 10 + j,  1 + (i - 1) * 50);
                JobSpecification job = mock(JobSpecification.class);
                IClusterCapacity requiredClusterCapacity = mockIClusterCapacity(4, (i - 1) * 10);
                when(job.getRequiredClusterCapacity()).thenReturn(requiredClusterCapacity);
                when(run.getJobSpecification()).thenReturn(job);
                when(jobCapacityController.allocate(job)).thenReturn(IJobCapacityController.JobSubmissionStatus.EXECUTE);
                when(run.getAddedToMemoryQueueTime()).thenReturn(time);
                queuedJobs.add(run);
                defaultJobQueue.add(run);
            }
        }
        System.out.println(defaultJobQueue.printQueueInfo());
        int counter = 0;
        while(!defaultJobQueue.isEmpty()) {
            List<JobRun> selected = defaultJobQueue.pull();
            JobRun jobRun = selected.get(0);
            System.out.println("Selected Job:" + jobRun.getJobId());
            defaultJobQueue.notifyJobFinished(jobRun);
            capacityControllerGuard.release(jobRun);
            time += jobRun.getExecutionTime();
            when(defaultJobQueue.getCurrentTime()).thenReturn(time);
            counter += 1;
            if (counter % 5 == 0) {
                System.out.println("Current Time: " + time);
                System.out.println(defaultJobQueue.printQueueInfo());
            }
        }
    }


        private JobRun mockDefaultJobRun(long id) {
        JobRun run = mock(JobRun.class, Mockito.RETURNS_DEEP_STUBS);
        when(run.getExceptions()).thenReturn(Collections.emptyList());
        when(run.getActivityClusterGraph().isReportTaskDetails()).thenReturn(true);
        when(run.getPendingExceptions()).thenReturn(Collections.emptyList());
        JobId jobId = new JobId(id);
        when(run.getJobId()).thenReturn(jobId);
        when(run.getSchedulingType()).thenReturn(JobTypeManager.JobSchedulingType.DEFAULT);

        Set<String> nodes = new HashSet<>();
        nodes.add("node1");
        nodes.add("node2");
        when(run.getParticipatingNodeIds()).thenReturn(nodes);
        when(run.getCleanupPendingNodeIds()).thenReturn(nodes);
        return run;
    }

    private JobRun mockJobRunWithExecutionTime(long id, long executionTime) {
        JobRun run = mock(JobRun.class, Mockito.RETURNS_DEEP_STUBS);
        when(run.getExceptions()).thenReturn(Collections.emptyList());
        when(run.getActivityClusterGraph().isReportTaskDetails()).thenReturn(true);
        when(run.getPendingExceptions()).thenReturn(Collections.emptyList());
        JobId jobId = new JobId(id);
        when(run.getJobId()).thenReturn(jobId);
        when(run.getSchedulingType()).thenReturn(JobTypeManager.JobSchedulingType.DEFAULT);
        when(run.getExecutionTime()).thenReturn(executionTime);

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
