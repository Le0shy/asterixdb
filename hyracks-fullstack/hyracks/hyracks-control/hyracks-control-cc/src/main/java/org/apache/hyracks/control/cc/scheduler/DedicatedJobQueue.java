package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;

import java.util.Collection;
import java.util.List;

public class DedicatedJobQueue implements IJobQueue{
    IJobQueue FIFOJobQueue;
    public DedicatedJobQueue(IJobManager jobManager, IJobCapacityController jobCapacityController) {
        FIFOJobQueue = new FIFOJobQueue(jobManager, jobCapacityController);
    }

    @Override
    public void add(JobRun run) throws HyracksException {
        FIFOJobQueue.add(run);
    }

    @Override
    public JobRun remove(JobId jobId) {
        return FIFOJobQueue.remove(jobId);
    }

    @Override
    public JobRun get(JobId jobId) {
        return FIFOJobQueue.remove(jobId);
    }

    @Override
    public List<JobRun> pull(JobTypeManager.JobSchedulingType schedulingType) {
        return null;
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
