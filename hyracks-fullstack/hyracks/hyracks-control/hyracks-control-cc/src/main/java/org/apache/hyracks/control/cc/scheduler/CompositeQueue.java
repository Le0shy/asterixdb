package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.cc.job.WorkloadManager;
import org.apache.hyracks.util.annotations.GuardedBy;
import org.apache.hyracks.util.annotations.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
@NotThreadSafe
@GuardedBy("JobManager")
public class CompositeQueue implements IJobQueue{
    private final CapacityControllerGuard capacityControllerGuard;
    //private final IJobCapacityController jobCapacityController;
    private final IJobQueue SQAJobQueue;
    private final IJobQueue priorityBasedQueue;

    public CompositeQueue(IJobManager jobManager, IJobCapacityController jobCapacityController) {
        SQAJobQueue = new DedicatedJobQueue(jobManager, jobCapacityController);
        priorityBasedQueue = new PriorityBasedQueue(jobManager, jobCapacityController);
        capacityControllerGuard = new CapacityControllerGuard(jobCapacityController);
    }
    @Override
    public void add(JobRun run) throws HyracksException {
        JobTypeManager.JobSchedulingType schedulingType = run.getSchedulingType();
        if (schedulingType == JobTypeManager.JobSchedulingType.SHORT) {
            SQAJobQueue.add(run);
        } else {
            priorityBasedQueue.add(run);
        }
    }

    @Override
    public JobRun remove(JobId jobId) {
        JobRun removedJob;
        removedJob = SQAJobQueue.remove(jobId);
        if (removedJob != null) {
            return removedJob;
        }
        return priorityBasedQueue.remove(jobId);
    }

    @Override
    public JobRun get(JobId jobId) {
        JobRun acquiredJob;
        acquiredJob = SQAJobQueue.get(jobId);
        if (acquiredJob != null) {
            return acquiredJob;
        }
        return priorityBasedQueue.get(jobId);
    }

    @Override
    public List<JobRun> pull() {
        //List<JobRun> dedicatedJobToRun = SQAJobQueue.pull(schedulingType);
        //List<JobRun> priorityBasedJobToRun = priorityBasedQueue.pull(schedulingType);
        List<JobRun> jobRuns;
        if (capacityControllerGuard.isSQAResourcesAvailable()){
            jobRuns = SQAJobQueue.pull();
        } else {
            jobRuns = new ArrayList<>();
        }
        jobRuns.addAll(priorityBasedQueue.pull());
        return jobRuns;
    }

    @Override
    public Collection<JobRun> jobs() {
        List<JobRun> jobs = new ArrayList<>();
        jobs.addAll(SQAJobQueue.jobs());
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
