package org.apache.hyracks.control.cc.scheduler;

import java.util.HashMap;

public class WorkloadConfig {
    private long defaultPriority;
    private double shortMemoryPercent;
    private int shortCPUQuota;
    private HashMap<String, Long> groupsToPriorities;

    public WorkloadConfig(long defaultPriority, double shortMemoryPercent, int shortCPUQuota,
            HashMap<String, Long> groupToPriorities) {
        this.defaultPriority = defaultPriority;
        this.shortMemoryPercent = shortMemoryPercent;
        this.shortCPUQuota = shortCPUQuota;
        this.groupsToPriorities = groupToPriorities;
    }

    public long getDefaultPriority() {
        return defaultPriority;
    }

    public void setDefaultPriority(long defaultPriority) {
        this.defaultPriority = defaultPriority;
    }

    public double getShortMemoryPercent() {
        return shortMemoryPercent;
    }

    public void setShortMemoryPercent(double shortMemoryPercent) {
        this.shortMemoryPercent = shortMemoryPercent;
    }

    public int getShortCPUQuota() {
        return shortCPUQuota;
    }

    public void setShortCPUQuota(int shortCPUQuota) {
        this.shortCPUQuota = shortCPUQuota;
    }

    public HashMap<String, Long> getGroupsToPriorities() {
        return groupsToPriorities;
    }

    public void setGroupsToPriorities(HashMap<String, Long> groupsToPriorities) {
        this.groupsToPriorities = groupsToPriorities;
    }
}
