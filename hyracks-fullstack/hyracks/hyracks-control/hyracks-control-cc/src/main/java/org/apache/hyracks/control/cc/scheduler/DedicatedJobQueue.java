package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class DedicatedJobQueue implements IJobQueue {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<JobId, JobRun> jobListMap = new LinkedHashMap<>();
    private final IJobManager jobManager;
    private final CapacityControllerGuard capacityControllerGuard;
    private final int jobQueueCapacity;

    public DedicatedJobQueue(IJobManager jobManager, CapacityControllerGuard capacityControllerGuard) {
        this.jobManager = jobManager;
        this.capacityControllerGuard = capacityControllerGuard;
        this.jobQueueCapacity = jobManager.getJobQueueCapacity();
    }

    @Override
    public void add(JobRun run) throws HyracksException {
        int size = jobListMap.size();
        if (size >= jobQueueCapacity) {
            throw HyracksException.create(ErrorCode.JOB_QUEUE_FULL, jobQueueCapacity);
        }
        jobListMap.put(run.getJobId(), run);
    }

    @Override
    public JobRun remove(JobId jobId) {
        return jobListMap.remove(jobId);
    }

    @Override
    public JobRun get(JobId jobId) {
        return jobListMap.get(jobId);
    }

    @Override
    public List<JobRun> pull() {
        List<JobRun> jobRuns = new ArrayList<>();
        Iterator<JobRun> runIterator = jobListMap.values().iterator();
        while (runIterator.hasNext()) {
            JobRun run = runIterator.next();
            // Cluster maximum capacity can change over time, thus we have to re-check if the job should be rejected
            // or not.
            try {
                IJobCapacityController.JobSubmissionStatus status = capacityControllerGuard.allocate(run);
                // Checks if the job can be executed immediately.
                if (status == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
                    jobRuns.add(run);
                    runIterator.remove(); // Removes the selected job.
                }
            } catch (HyracksException exception) {
                // The required capacity exceeds maximum capacity.
                List<Exception> exceptions = new ArrayList<>();
                exceptions.add(exception);
                runIterator.remove(); // Removes the job from the queue.
                try {
                    // Fails the job.
                    jobManager.prepareComplete(run, JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
                } catch (HyracksException e) {
                    LOGGER.log(Level.ERROR, e.getMessage(), e);
                }
            }
        }
        return jobRuns;
    }

    @Override
    public Collection<JobRun> jobs() {
        return Collections.unmodifiableCollection(jobListMap.values());
    }

    @Override
    public void clear() {
        jobListMap.clear();
    }

    @Override
    public void notifyJobFinished(JobRun run) {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
