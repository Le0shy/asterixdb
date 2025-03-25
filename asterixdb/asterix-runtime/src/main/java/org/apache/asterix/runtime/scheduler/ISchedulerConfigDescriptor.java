package org.apache.asterix.runtime.scheduler;

import java.util.List;
import java.util.Map;

public interface ISchedulerConfigDescriptor {
    String getName();

    long getDefaultPriority();

    double getShortMemoryPercent();

    long getShortCPUQuota();

    Map<String, Long> getGroupToPriority();

    Map<Long, List<String>> getPriorityToGroup();

    void upsertQueryGroup(Map<String, Long> upsertQueryGroups);

    boolean deleteQueryGroup(List<String> deleteQueryGroups);
}
