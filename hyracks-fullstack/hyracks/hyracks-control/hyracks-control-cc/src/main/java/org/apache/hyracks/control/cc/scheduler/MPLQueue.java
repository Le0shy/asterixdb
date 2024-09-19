package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.job.JobRun;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class MPLQueue {
    private final Map<JobId, JobRun> jobs = new LinkedHashMap<>();
    private double topQuerySlowDown;
    private long sumExecutionTimesIncludingQueueTime = 0L;
    private int countOfExecutedJobs = 0;
    private Iterator<Map.Entry<JobId, JobRun>> it;
    private double candidateQueryExecTime = 1;
    private int id;
    private int jobQueueCapacity;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("queue_id: " + id + ",\n");
        sb.append("jobs:{ ");
        for (JobId jid : jobs.keySet()) {
            sb.append(jid + ",");
        }
        sb.append("}");
        return sb.toString();
    }

    public MPLQueue(int id, int jobQueueCapacity) {
        this.id = id;
        this.jobQueueCapacity = jobQueueCapacity;
    }

    public MPLQueue(int id, double candidateQueryExecTime, int jobQueueCapacity) {
        this.id = id;
        this.candidateQueryExecTime = candidateQueryExecTime;
        this.jobQueueCapacity = jobQueueCapacity;
    }

    public int getQueueSize() {
        return jobs.size();
    }

    public void put(JobId id, JobRun run) throws HyracksException {
        if (getQueueSize() >= jobQueueCapacity) {
            throw HyracksException.create(ErrorCode.JOB_QUEUE_FULL, jobQueueCapacity);
        }
        this.jobs.put(id, run);
    }

    public JobRun remove(JobId id) {
        return jobs.remove(id);
    }

    public JobRun get(JobId id) {
        return jobs.get(id);
    }

    public JobRun getFirst() {
        it = jobs.entrySet().iterator();
        if (it.hasNext()) {
            return it.next().getValue();
        }
        return null;
    }

    public int getId() {
        return id;
    }
}
