package org.apache.asterix.runtime.scheduler;

import java.util.List;
import java.util.Map;

public interface ISchedulerConfigDescriptor {
    String getName();

    public long getDefaultPriority();

    public double getShortMemoryPercent();

    public long getShortCPUQuota();

    public Map<String, Long> getGroupToPriority();

    public Map<Long, List<String>> getPriorityToGroup();
}
