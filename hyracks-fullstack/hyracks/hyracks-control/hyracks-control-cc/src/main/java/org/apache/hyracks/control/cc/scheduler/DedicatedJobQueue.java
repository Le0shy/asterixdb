package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.job.JobRun;

import java.util.Collection;
import java.util.List;

public class DedicatedJobQueue implements IJobQueue{
    @Override
    public void add(JobRun run) throws HyracksException {

    }

    @Override
    public JobRun remove(JobId jobId) {
        return null;
    }

    @Override
    public JobRun get(JobId jobId) {
        return null;
    }

    @Override
    public List<JobRun> pull() {
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
