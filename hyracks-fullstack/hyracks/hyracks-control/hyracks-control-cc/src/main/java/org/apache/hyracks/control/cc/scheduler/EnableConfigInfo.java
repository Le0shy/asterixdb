package org.apache.hyracks.control.cc.scheduler;

import java.util.HashMap;
import java.util.Map;

public class EnableConfigInfo implements IWorkloadConfigInfo {
    private long defaultPriority;
    private double shortMemoryPercent;
    private int shortCPUQuota;
    private Map<String, Long> groupsToPriorities;
    private IWorkloadConfigInfo.Type type;

    public EnableConfigInfo(long defaultPriority, double shortMemoryPercent, int shortCPUQuota,
            Map<String, Long> groupToPriorities) {
        this.defaultPriority = defaultPriority;
        this.shortMemoryPercent = shortMemoryPercent;
        this.shortCPUQuota = shortCPUQuota;
        this.groupsToPriorities = groupToPriorities;
        this.type = IWorkloadConfigInfo.Type.ENABLE_CONFIG;
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

    public Map<String, Long> getGroupsToPriorities() {
        return groupsToPriorities;
    }

    public IWorkloadConfigInfo.Type getType() {
        return type;
    }

    public void setGroupsToPriorities(HashMap<String, Long> groupsToPriorities) {
        this.groupsToPriorities = groupsToPriorities;
    }
}
