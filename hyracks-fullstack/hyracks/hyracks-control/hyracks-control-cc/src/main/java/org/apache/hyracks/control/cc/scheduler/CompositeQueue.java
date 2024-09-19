package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.util.annotations.GuardedBy;
import org.apache.hyracks.util.annotations.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
@NotThreadSafe
@GuardedBy("JobManager")
public class CompositeQueue implements IJobQueue{
    private final IJobQueue dedicatedJobQueue;
    private final IJobQueue priorityBasedQueue;

    public CompositeQueue(IJobManager jobManager, IJobCapacityController jobCapacityController) {
        dedicatedJobQueue = new FIFOJobQueue(jobManager, jobCapacityController);
        priorityBasedQueue = new PriorityBasedQueue(jobManager, jobCapacityController);
    }
    @Override
    public void add(JobRun run) throws HyracksException {
        JobTypeManager.JobSchedulingType schedulingType = run.getSchedulingType();
        if (schedulingType == JobTypeManager.JobSchedulingType.SHORT) {
            dedicatedJobQueue.add(run);
        } else {
            priorityBasedQueue.add(run);
        }
    }

    @Override
    public JobRun remove(JobId jobId) {
        JobRun removedJob;
        removedJob = dedicatedJobQueue.remove(jobId);
        if (removedJob != null) {
            return removedJob;
        }
        return priorityBasedQueue.remove(jobId);
    }

    @Override
    public JobRun get(JobId jobId) {
        JobRun acquiredJob;
        acquiredJob = dedicatedJobQueue.get(jobId);
        if (acquiredJob != null) {
            return acquiredJob;
        }
        return priorityBasedQueue.get(jobId);
    }

    @Override
    public List<JobRun> pull() {
        List<JobRun> dedicatedJobToRun = dedicatedJobQueue.pull();
        List<JobRun> priorityBasedJobToRun = priorityBasedQueue.pull();
        return null;
    }

    @Override
    public Collection<JobRun> jobs() {
        List<JobRun> jobs = new ArrayList<>();
        jobs.addAll(dedicatedJobQueue.jobs());
        jobs.addAll(priorityBasedQueue.jobs());
        return Collections.unmodifiableCollection(jobs);
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
