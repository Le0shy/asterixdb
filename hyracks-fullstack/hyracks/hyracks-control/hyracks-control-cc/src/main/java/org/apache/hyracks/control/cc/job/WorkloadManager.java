package org.apache.hyracks.control.cc.job;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.scheduler.IJobTypeManager;
import org.apache.hyracks.control.cc.scheduler.JobTypeManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkloadManager extends JobManager{

    private static final Logger LOGGER = LogManager.getLogger();
    private final IJobTypeManager jobTypeManager = new JobTypeManager();
    public WorkloadManager(CCConfig ccConfig, ClusterControllerService ccs,
            IJobCapacityController jobCapacityController) {
        super(ccConfig, ccs, jobCapacityController);
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
        //Whenever a new jobs added or a job finishes, check for jobs in the queue that can execite with the
        // current resources
        pickJobsToRun();
    }

    @Override
    public int getDefaultQueuePriority() {
        return jobTypeManager.getDefaultPriority();
    }

}
