package org.apache.asterix.runtime.scheduler;

import org.apache.asterix.common.metadata.DataverseName;

public class SchedulerConfigStateDescriptor implements ISchedulerConfigDescriptor {
    private final String databaseName;
    private final DataverseName dataverseName;
    private final String name;
    private String enabledConfigName;

    public SchedulerConfigStateDescriptor(String databaseName, DataverseName dataverseName, String name,
            String enabledConfigName) {
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.name = name;
        this.enabledConfigName = enabledConfigName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getEnabledConfigName() {
        return enabledConfigName;
    }

    public void setEnabledConfigName(String setEnabledConfigName) {
        enabledConfigName = setEnabledConfigName;
    }
}
