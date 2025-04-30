package org.apache.hyracks.control.cc.job;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.scheduler.*;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.work.NoOpCallback;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkloadManager extends JobManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final IJobTypeManager jobTypeManager = new JobTypeManager();
    private final CapacityControllerGuard workloadCapacityController;

    public WorkloadManager(CCConfig ccConfig, ClusterControllerService ccs,
            IJobCapacityController jobCapacityController) {
        super(ccConfig, ccs, jobCapacityController);
        workloadCapacityController = new CapacityControllerGuard(jobCapacityController);
        jobQueue = new CompositeQueue(this, workloadCapacityController);
    }

    @Override
    public void add(JobRun jobRun) throws HyracksException {
        //All newly added jobs should get queued
        if (jobRun == null) {
            throw HyracksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
        }
        /* Determine the type and assign corresponding job priority */
        jobTypeManager.setJobType(jobRun);
        JobSpecification job = jobRun.getJobSpecification();
        CCServiceContext serviceCtx = ccs.getContext();
        serviceCtx.notifyJobCreation(jobRun.getJobId(), job, IJobCapacityController.JobSubmissionStatus.QUEUE);
        queueJob(jobRun);
        /* Whenever a new jobs added or a job finishes, check for jobs in the queue that can execute with the
            current resources */
        pickJobsToRun();
    }

    @Override
    public void finalComplete(JobRun run) throws HyracksException {
        checkJob(run);
        boolean successful = run.getPendingStatus() == JobStatus.TERMINATED;

        JobId jobId = run.getJobId();
        Throwable caughtException = null;
        CCServiceContext serviceCtx = ccs.getContext();
        try {
            serviceCtx.notifyJobFinish(jobId, run.getJobSpecification(), run.getPendingStatus(),
                    run.getPendingExceptions());
        } catch (Exception e) {
            LOGGER.error("Exception notifying job finish {}", jobId, e);
            caughtException = e;
        }
        run.setStatus(run.getPendingStatus(), run.getPendingExceptions());
        run.setEndTime(System.currentTimeMillis());
        run.setExecutionEndTime(System.nanoTime());
        if (activeRunMap.remove(jobId) != null) {
            incrementJobCounters(run, successful);

            // non-active jobs have zero capacity
            releaseJobCapacity(run);
        }
        runMapArchive.put(jobId, run);
        runMapHistory.put(jobId, run.getExceptions());

        if (run.getActivityClusterGraph().isReportTaskDetails()) {
            /*
             * log job details when profiling is enabled
             */
            try {
                ccs.getJobLogFile().log(createJobLogObject(run));
            } catch (Exception e) {
                LOGGER.error("Exception reporting task details for job {}", jobId, e);
                caughtException = ExceptionUtils.suppress(caughtException, e);
            }
        }
        jobQueue.notifyJobFinished(run);
        // Picks the next job to execute.
        pickJobsToRun();

        // throws caught exceptions if any
        if (caughtException != null) {
            throw HyracksException.wrapOrThrowUnchecked(caughtException);
        }
    }

    private void executeJob(JobRun run) throws HyracksException {
        run.setStartTime(System.currentTimeMillis());
        run.setStartTimeZoneId(ZoneId.systemDefault().getId());
        JobId jobId = run.getJobId();
        logJobCapacity(run, "running", Level.DEBUG);
        activeRunMap.put(jobId, run);
        run.setStatus(JobStatus.RUNNING, null);
        executeJobInternal(run);
    }

    private void executeJobInternal(JobRun run) {
        try {
            run.getExecutor().startJob();
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Aborting " + run.getJobId() + " due to failure during job start", e);
            final List<Exception> exceptions = Collections.singletonList(e);
            // fail the job then abort it
            run.setStatus(JobStatus.FAILURE, exceptions);
            // abort job will trigger JobCleanupWork
            run.getExecutor().abortJob(exceptions, NoOpCallback.INSTANCE);
        }
    }

    private void logJobCapacity(JobRun jobRun, String jobStateDesc, Level lvl) {
        IClusterCapacity requiredResources = jobRun.getJobSpecification().getRequiredClusterCapacity();
        if (requiredResources == null) {
            return;
        }
        long requiredMemory = requiredResources.getAggregatedMemoryByteSize();
        int requiredCPUs = requiredResources.getAggregatedCores();
        if (requiredMemory == 0 && requiredCPUs == 0) {
            return;
        }
        IReadOnlyClusterCapacity clusterCapacity = jobCapacityController.getClusterCapacity();
        LOGGER.log(lvl, "{} {}, memory={}, cpu={}, (new) cluster memory={}, cpu={}, currently running={}, queued={}",
                jobStateDesc, jobRun.getJobId(), requiredMemory, requiredCPUs,
                clusterCapacity.getAggregatedMemoryByteSize(), clusterCapacity.getAggregatedCores(),
                getRunningJobsCount(), jobQueue.size());
    }

    private void releaseJobCapacity(JobRun jobRun) {
        workloadCapacityController.release(jobRun);
        logJobCapacity(jobRun, "released", Level.DEBUG);
    }

    private void pickJobsToRun() throws HyracksException {
        List<JobRun> selectedRuns = jobQueue.pull();
        for (JobRun run : selectedRuns) {
            executeJob(run);
        }
    }

    public long getDefaultQueuePriority() {
        return jobTypeManager.getDefaultPriority();
    }

    public void enableSchedulerConfig(EnableConfigInfo enableConfig) {
        jobTypeManager.setWorkloadConfig(enableConfig.getGroupsToPriorities());
        setWorkloadParameters(enableConfig);
    }

    public void addQueryGroups(Map<String, Long> groupsToAdd) {
        jobTypeManager.addGroups(groupsToAdd);
    }

    public void removeQueryGroups(List<String> groupsToRemove) {
        jobTypeManager.removeGroups(groupsToRemove);
    }

    public void setWorkloadParameters(EnableConfigInfo workloadConfig) {
        jobTypeManager.setDefaultPriority(workloadConfig.getDefaultPriority());
        workloadCapacityController.setCPUQuota(workloadConfig.getShortCPUQuota());
        workloadCapacityController.setMemoryPercentAllocatedToShort(workloadConfig.getShortMemoryPercent());
    }

}
