package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.job.JobRun;
import org.ini4j.Registry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobTypeManager implements IJobTypeManager {

    private Map<String, Long> queryGroupToPriority;
    private long defaultPriority = 1;
    public enum JobSchedulingType {
        SHORT,
        LONG,
        NORMAL,
        DEFAULT,
        METADATA
    }

    /* TODO: get config while system bootstraps
       fetching from metadata provider or otherwise? */
    public JobTypeManager() {
        queryGroupToPriority = new HashMap<>();
    }

    @Override
    public void setJobType(JobRun jobRun) {
        JobSpecification job = jobRun.getJobSpecification();
        int jobPriority = job.getPriority();
        if (jobPriority == 0) {
            jobRun.setSchedulingType(JobSchedulingType.SHORT);
        }
        /* else if (jobPriority == 0) {
            jobRun.setSchedulingType(JobSchedulingType.LONG);
        } */
        else if (jobPriority == defaultPriority) {
            jobRun.setSchedulingType(JobSchedulingType.DEFAULT);
        } else {
            jobRun.setSchedulingType(JobSchedulingType.NORMAL);
        }
        jobRun.setPriority(jobPriority);
    }

    @Override
    public void setDefaultPriority(long dp) {
        defaultPriority = dp;
    }

    @Override
    public void setWorkloadConfig(HashMap<String, Long> groupsToPriorities) {
        queryGroupToPriority = groupsToPriorities;
    }

    @Override
    public void addGroups(Map<String, Long> groupsToAdd) {
        for(String groupName: groupsToAdd.keySet()) {
            queryGroupToPriority.put(groupName, groupsToAdd.get(groupName));
        }
    }

    @Override
    public void removeGroups(List<String> groupsToRemove) {
        for(String groupName: groupsToRemove) {
            queryGroupToPriority.remove(groupName);
        }
    }

    @Override
    public long getDefaultPriority() {
        return defaultPriority;
    }


}
