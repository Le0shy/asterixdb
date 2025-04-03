package org.apache.asterix.metadata.utils;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.runtime.scheduler.SchedulerConfigDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class SchedulerUtil {

    public static SchedulerConfigDescriptor fetchSchedulerConfigDescriptor(
            MetadataProvider metadataProvider,
            String database, DataverseName dataverseName, String configName) throws AlgebricksException {
        SchedulerConfigDescriptor configDescriptor =
                metadataProvider.findSchedulerConfig(database, dataverseName, configName).getSchedulerConfig();

        return configDescriptor;
    }
}
