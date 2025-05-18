package org.apache.hyracks.control.cc.scheduler;

import java.util.*;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.cc.job.WorkloadManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PriorityBasedQueue implements IJobQueue {
    private final Logger LOGGER = LogManager.getLogger();
    private final IJobQueue defaultJobQueue;
    private final IJobManager jobManager;
    private final CapacityControllerGuard capacityControllerGuard;
    /* default queue's priority can be modified */
    private long defaultQueuePriority;
    private final Map<JobId, MPLQueue> jobIdToQueueMap = new HashMap<>();
    private final Random random = new Random();
    private final Map<Integer, MPLQueue> queues;
    private final Set<Integer> activeQueues;

    public PriorityBasedQueue(IJobManager jobManager, CapacityControllerGuard capacityControllerGuard) {
        defaultJobQueue = new DefaultJobQueue(jobManager, capacityControllerGuard);
        queues = new HashMap<>();
        activeQueues = new HashSet<>();
        defaultQueuePriority = ((WorkloadManager) jobManager).getDefaultQueuePriority();
        this.jobManager = jobManager;
        this.capacityControllerGuard = capacityControllerGuard;
    }

    @Override
    public void add(JobRun run) throws HyracksException {
        JobTypeManager.JobSchedulingType jobSchedulingType = run.getSchedulingType();
        /* if the job's scheduling type is DEFAULT OR LONG, add it to the default queue */
        if (jobSchedulingType == JobTypeManager.JobSchedulingType.DEFAULT
                || jobSchedulingType == JobTypeManager.JobSchedulingType.LONG) {
            defaultJobQueue.add(run);
        } else {
            /* add it to its corresponding priority based queue */
            int jobPriority = run.getPriority();
            MPLQueue targetQueue;
            if (queues.containsKey(jobPriority)) {
                targetQueue = queues.get(jobPriority);
            } else {
                targetQueue = new MPLQueue(jobPriority, jobManager.getJobQueueCapacity());
            }
            targetQueue.put(run.getJobId(), run);
            queues.put(jobPriority, targetQueue);
            /* Jobs are waiting in the queue */
            activeQueues.add(jobPriority);
            jobIdToQueueMap.put(run.getJobId(), targetQueue);
            run.setAddedToQueueTime(getCurrentTime());
            LOGGER.log(Level.DEBUG, "JobID:{}, scheduled to PriorityBasedQueue, group:{}, priority{}",
                    run.getJobId(), run.getJobSpecification().getGroupName(), run.getPriority());

            LOGGER.log(Level.DEBUG, "{}", printQueueInfo());
        }
    }

    public JobRun remove(JobId jobId) {
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
    private int searchSelected(long[] distribution, long target) {
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
            IJobCapacityController.JobSubmissionStatus status = capacityControllerGuard.allocate(nextToRun);
            /* Checks if the job can be executed immediately. */
            if (status == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
                jobRuns.add(nextToRun);
                /* remove it from its queue */
                remove(nextToRun.getJobId());
            }
        } catch (HyracksException exception) {
            /* The required capacity exceeds maximum capacity. */
            List<Exception> exceptions = new ArrayList<>();
            exceptions.add(exception);
            try {
                /* Job fails before execution */
                jobManager.prepareComplete(nextToRun, JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
                remove(nextToRun.getJobId());
            } catch (HyracksException e) {
                LOGGER.log(Level.ERROR, e.getMessage(), e);
            }
        }
    }

    @Override
    public List<JobRun> pull() {
        /* Generate probability distribution based on queues' (those have jobs waiting) priorities */
        List<JobRun> jobRuns = new ArrayList<>();
        int numNonEmptyQueues = activeQueues.size();
        if (!defaultJobQueue.isEmpty()) {
            numNonEmptyQueues += 1;
        }
        if (numNonEmptyQueues == 0) {
            return jobRuns;
        }
        long[] distribution = new long[numNonEmptyQueues + 1];
        distribution[0] = 0;
        if (!defaultJobQueue.isEmpty()) {
            distribution[1] = defaultQueuePriority;
            int iter = 2;
            for (Integer priority : activeQueues) {
                distribution[iter] = priority + distribution[iter - 1];
                iter += 1;
            }
        } else {
            int iter = 1;
            for (Integer priority : activeQueues) {
                if (iter == 1) {
                    distribution[iter] = priority;
                } else {
                    distribution[iter] = priority + distribution[iter - 1];
                }
                iter += 1;
            }
        }

        long generatedRandomInteger = random.nextLong(1, distribution[numNonEmptyQueues] + 1);
        int selectedIdx = searchSelected(distribution, generatedRandomInteger);

        if (!defaultJobQueue.isEmpty() && selectedIdx == 1) {
            /* default queue gets selected */
            return defaultJobQueue.pull();
        } else {
            /* other queue gets selected */
            int targetedPriority = (int) (distribution[selectedIdx] - distribution[selectedIdx - 1]);
            MPLQueue selectedQueue = queues.get(targetedPriority);
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
    public int size() {
        return jobIdToQueueMap.size();
    }

    @Override
    public void notifyJobFinished(JobRun run) {
        if (run.getSchedulingType() == JobTypeManager.JobSchedulingType.NORMAL) {
            jobIdToQueueMap.remove(run.getJobId());
        } else {
            defaultJobQueue.notifyJobFinished(run);
        }
    }

    public String printQueueInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nPriority Based Queues:{\n");
        for (Integer priority : queues.keySet()) {
            sb.append(queues.get(priority)).append("\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

    @Override
    public boolean isEmpty() {
        return activeQueues.isEmpty();
    }
}
