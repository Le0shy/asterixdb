package org.apache.hyracks.control.cc.scheduler;

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

public class PriorityBasedQueue implements IJobQueue{
    private final Logger LOGGER = LogManager.getLogger();
    private final IJobQueue defaultJobQueue;
    private final IJobManager jobManager;
    private final IJobCapacityController jobCapacityController;
    /* default queue's priority can be modified */
    private int defaultQueuePriority;
    private final Map<JobId, MPLQueue> jobIdToQueueMap = new HashMap<>();
    private final Random random = new Random();
    private final Map<Integer, MPLQueue> queues;
    private final Set<Integer> activeQueues;

    public PriorityBasedQueue(IJobManager jobManager, IJobCapacityController jobCapacityController) {
        defaultJobQueue = new DefaultJobQueue(jobManager, jobCapacityController);
        queues = new HashMap<>();
        activeQueues = new HashSet<>();
        defaultQueuePriority = jobManager.getDefaultQueuePriority();
        this.jobManager = jobManager;
        this.jobCapacityController = jobCapacityController;
    }
    @Override
    public void add(JobRun run) throws HyracksException {
        JobTypeManager.JobSchedulingType jobSchedulingType = run.getSchedulingType();
        /* if the job's scheduling type is DEFAULT OR LONG, add it to the default queue */
        if (jobSchedulingType == JobTypeManager.JobSchedulingType.DEFAULT ||
        jobSchedulingType == JobTypeManager.JobSchedulingType.LONG) {
            defaultJobQueue.add(run);
        } else {
            /* add it to its corresponding priority based queue */
            int jobPriority = run.getPriority();
            MPLQueue targetQueue;
            if (queues.containsKey(jobPriority)) {
                targetQueue = queues.get(jobPriority);
                targetQueue.put(run.getJobId(), run);
            } else {
                targetQueue = new MPLQueue(jobPriority, jobManager.getJobQueueCapacity());
                targetQueue.put(run.getJobId(), run);
                queues.put(jobPriority, targetQueue);
                /* Jobs are waiting in the queue */
                activeQueues.add(jobPriority);
                jobIdToQueueMap.put(run.getJobId(), targetQueue);
            }
        }
    }
    private JobRun removeFromNormalQueue(JobId jobId) {
        MPLQueue queue = jobIdToQueueMap.get(jobId);
        if (queue == null) {
            return null;
        }
        JobRun ret = queue.remove(jobId);
        /* no jobs are waiting */
        if (ret != null && queue.getFirst() == null) {
            activeQueues.remove(ret.getPriority());
        }
        return ret;
    }
    @Override
    public JobRun remove(JobId jobId) {
        JobRun removedJob;
        removedJob = defaultJobQueue.remove(jobId);
        if (removedJob != null) {
            return removedJob;
        }

        return removeFromNormalQueue(jobId);
    }

    @Override
    public JobRun get(JobId jobId) {
        JobRun targetJob;
        targetJob = defaultJobQueue.get(jobId);
        if (targetJob != null) {
            return targetJob;
        }

        MPLQueue queue = jobIdToQueueMap.get(jobId);
        if (queue == null) {
            return null;
        }
        return queue.get(jobId);
    }

    /* Binary search for left boundary: it returns idx whose values is the smallest number
    greater than 'target' when idx not exists. */
    private int searchSelected(int[] distribution, int target) {
        int left = 0;
        int right = distribution.length;
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (distribution[mid] == target) {
                right = mid;
            } else if (distribution[mid] < target) {
                left = mid + 1;
            } else if (distribution[mid] > target) {
                right = mid;
            }
        }
        return left;
    }

    private void checkAndAdd(JobRun nextToRun, List<JobRun> jobRuns) {
        if (nextToRun == null) {
            return;
        }
        try {
            IJobCapacityController.JobSubmissionStatus status =
                    jobCapacityController.allocate(nextToRun.getJobSpecification());
            /* Checks if the job can be executed immediately. */
            if (status == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
                jobRuns.add(nextToRun);
                /* remove it from its queue */
                removeFromNormalQueue(nextToRun.getJobId());
            }
        } catch (HyracksException exception) {
            /* The required capacity exceeds maximum capacity. */
            List<Exception> exceptions = new ArrayList<>();
            exceptions.add(exception);
            try {
                /* Job fails before execution */
                jobManager.prepareComplete(nextToRun, JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
                removeFromNormalQueue(nextToRun.getJobId());
            } catch (HyracksException e) {
                LOGGER.log(Level.ERROR, e.getMessage(), e);
            }
        }
    }
    @Override
    public List<JobRun> pull(JobTypeManager.JobSchedulingType schedulingType) {
        /* Generate probability distribution based on queues' (those have jobs waiting) priorities */
        List<JobRun> jobRuns = new ArrayList<>();
        int numNonEmptyQueues = activeQueues.size();
        if (!defaultJobQueue.isEmpty()) {
           numNonEmptyQueues += 1;
        }
        int[] distribution = new int[numNonEmptyQueues];
        if (!defaultJobQueue.isEmpty()) {
            distribution[0] = defaultQueuePriority;
        }
        int iter = 1;
        for (Integer priority: activeQueues) {
            distribution[iter] = priority + distribution[iter - 1];
            iter += 1;
        }
        int generatedRandomInteger = random.nextInt(1, distribution[iter - 1] + 1);
        int selectedIdx = searchSelected(distribution, generatedRandomInteger);

        if (selectedIdx == 0) {
            /* default queue gets selected */
            return defaultJobQueue.pull(schedulingType);
        } else {
            /* other queue gets selected */
            MPLQueue selectedQueue = queues.get(distribution[selectedIdx] - distribution[selectedIdx - 1]);
            JobRun nextToRun = selectedQueue.getFirst();
            checkAndAdd(nextToRun, jobRuns);
            return jobRuns;
        }
    }

    @Override
    public Collection<JobRun> jobs() {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public void notifyJobFinished(JobRun run) {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
