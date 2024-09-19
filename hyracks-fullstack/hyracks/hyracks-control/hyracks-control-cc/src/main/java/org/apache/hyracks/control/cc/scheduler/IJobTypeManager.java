package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.control.cc.job.JobRun;

public interface IJobTypeManager {

    void setJobType(JobRun jobRun);

    int getDefaultPriority();
}
