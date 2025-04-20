package org.apache.asterix.runtime.scheduler;

public class SchedulerConfigStateDescriptor implements ISchedulerConfigDescriptor {
    private final String name;
    private String enabledConfigName;

    public SchedulerConfigStateDescriptor(String name, String enabledConfigName) {
        this.name = name;
        this.enabledConfigName = enabledConfigName;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getEnabledConfigName() {
        return enabledConfigName;
    }

    public void setEnabledConfigName(String setEnabledConfigName) {
        enabledConfigName = setEnabledConfigName;
    }
}
