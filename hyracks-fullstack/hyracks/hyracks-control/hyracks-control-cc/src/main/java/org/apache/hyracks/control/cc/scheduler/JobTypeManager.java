package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.job.JobRun;

public class JobTypeManager implements IJobTypeManager {
    int defaultPriority = 1;
    public enum JobSchedulingType {
        SHORT,
        LONG,
        NORMAL,
        DEFAULT
    }

    @Override
    public void setJobType(JobRun jobRun) {
        JobSpecification job = jobRun.getJobSpecification();
        int jobPriority = job.getPriority();
        if (jobPriority < 0) {
            jobRun.setSchedulingType(JobSchedulingType.SHORT);
        } else if (jobPriority == 0) {
            jobRun.setSchedulingType(JobSchedulingType.LONG);
        } else if (jobPriority == defaultPriority) {
            jobRun.setSchedulingType(JobSchedulingType.DEFAULT);
        } else {
            jobRun.setSchedulingType(JobSchedulingType.NORMAL);
        }
        jobRun.setPriority(jobPriority);
    }

    public int getDefaultPriority() {
        return defaultPriority;
    }
}
