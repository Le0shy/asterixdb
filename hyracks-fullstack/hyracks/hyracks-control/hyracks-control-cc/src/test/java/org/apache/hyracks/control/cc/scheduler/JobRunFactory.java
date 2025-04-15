package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.job.JobRun;

public class JobRunFactory {
    public static JobRun mockPrioritizedJob(long id, int priority, JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.NORMAL).withPriority(priority)
                .withJobSpecification(js).build();
    }

    public static JobRun mockPrioritizedJobWithExecutionTime(long id, long executionTime, int priority,
            JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.NORMAL).withPriority(priority)
                .withExecutionTime(executionTime).withJobSpecification(js).build();
    }

    public static JobRun mockDefaultJob(long id, long executionTime, JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.DEFAULT)
                .withExecutionTime(executionTime).withJobSpecification(js).build();
    }

    public static JobRun mockShortJob(long id, JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.SHORT).withJobSpecification(js)
                .build();
    }

    public static JobRun mockShortJobWithExecutionTime(long id, long executionTime, JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.SHORT)
                .withExecutionTime(executionTime).withJobSpecification(js).build();
    }
}
