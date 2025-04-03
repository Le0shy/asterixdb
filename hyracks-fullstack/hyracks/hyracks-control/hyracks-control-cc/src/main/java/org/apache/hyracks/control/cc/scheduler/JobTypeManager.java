package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.job.JobRun;

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
       fetching from metadata provider or otherwise?
    public JobManager() {

    }

    */


    @Override
    public void setJobType(JobRun jobRun) {
        /* *
        * TODO: add field to job specification;
        *       if the job is to change defaultPriority/ queryGroupToPriority
        * might call deleteGroup/upsertGroup accordingly
        * */

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

    public boolean deleteGroup(String groupName) {
        /* TODO */
        return false;
    }

    public void upsertGroup(String groupName, long priority) {
        /* TODO */
    }

    public void setDefaultPriority(long dp) {
        defaultPriority = dp;
    }

    public long getDefaultPriority() {
        return defaultPriority;
    }
}
