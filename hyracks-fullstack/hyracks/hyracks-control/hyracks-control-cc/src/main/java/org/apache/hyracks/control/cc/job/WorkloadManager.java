package org.apache.hyracks.control.cc.job;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.scheduler.CapacityControllerGuard;
import org.apache.hyracks.control.cc.scheduler.CompositeQueue;
import org.apache.hyracks.control.cc.scheduler.IJobTypeManager;
import org.apache.hyracks.control.cc.scheduler.JobTypeManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

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
        //Whenever a new jobs added or a job finishes, check for jobs in the queue that can execute with the
        // current resources
        pickJobsToRun();
    }
    @Override
    public void finalComplete(JobRun run) throws HyracksException {
        checkJob(run);
        if (run.getPendingStatus() == JobStatus.FAILURE) {
            incrementFailedJobs();
        } else if (run.getPendingStatus() == JobStatus.TERMINATED) {
            incrementSuccessfulJobs();
        }

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


    private void releaseJobCapacity(JobRun jobRun) {
        workloadCapacityController.release(jobRun);
    }

//    private void pickJobsToRun(JobRun jobRun) throws HyracksException{
//        if (jobRun.getSchedulingType() == JobTypeManager.JobSchedulingType.SHORT) {
//            pickShortJobs();
//        } else {
//            pickOtherJobs();
//        }
//    }


    private void pickJobsToRun() throws HyracksException {
        List<JobRun> selectedRuns = jobQueue.pull();
        for (JobRun run : selectedRuns) {
            executeJob(run);
        }
    }

    @Override
    public int getDefaultQueuePriority() {
        return jobTypeManager.getDefaultPriority();
    }

}
