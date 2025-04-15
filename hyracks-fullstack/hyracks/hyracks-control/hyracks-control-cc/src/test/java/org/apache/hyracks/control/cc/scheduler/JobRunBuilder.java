package org.apache.hyracks.control.cc.scheduler;

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.job.JobRun;
import org.mockito.Mockito;

public class JobRunBuilder {
    private final JobRun run;

    public JobRunBuilder(long id) {
        this.run = Mockito.mock(JobRun.class, Mockito.RETURNS_DEEP_STUBS);
        when(run.getExceptions()).thenReturn(Collections.emptyList());
        when(run.getActivityClusterGraph().isReportTaskDetails()).thenReturn(true);
        when(run.getPendingExceptions()).thenReturn(Collections.emptyList());
        JobId jobId = new JobId(id);
        when(run.getJobId()).thenReturn(jobId);
        Set<String> nodes = new HashSet<>();
        nodes.add("node1");
        nodes.add("node2");
        when(run.getParticipatingNodeIds()).thenReturn(nodes);
        when(run.getCleanupPendingNodeIds()).thenReturn(nodes);
    }

    public JobRunBuilder withSchedulingType(JobTypeManager.JobSchedulingType jst) {
        when(run.getSchedulingType()).thenReturn(jst);
        return this;
    }

    public JobRunBuilder withExecutionTime(long executionTime) {
        when(run.getExecutionTime()).thenReturn(executionTime);
        return this;
    }

    public JobRunBuilder withPriority(int priority) {
        when(run.getPriority()).thenReturn(priority);
        return this;
    }

    public JobRunBuilder withJobSpecification(JobSpecification js) {
        when(run.getJobSpecification()).thenReturn(js);
        return this;
    }

    public JobRun build() {
        return this.run;
    }

}
