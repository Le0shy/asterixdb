package org.apache.hyracks.control.cc.scheduler;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.control.cc.job.JobRun;

public interface IJobTypeManager {

    void setJobType(JobRun jobRun);

    long getDefaultPriority();

    void setDefaultPriority(long defaultPriority);

    void setWorkloadConfig(Map<String, Long> groupToPriorities);

    void addGroups(Map<String, Long> groupsToAdd);

    void removeGroups(List<String> groupsToRemove);
}
