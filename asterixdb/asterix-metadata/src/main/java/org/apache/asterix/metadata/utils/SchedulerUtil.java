package org.apache.asterix.metadata.utils;

import static org.apache.asterix.metadata.entities.SchedulerConfigMetadataEntity.SCHEDULER_DEFAULT_CONFIG_NAME;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.SchedulerConfigMetadataEntity;
import org.apache.asterix.runtime.scheduler.SchedulerConfigRecordDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.control.cc.scheduler.EnableConfigInfo;
import org.apache.hyracks.control.cc.scheduler.IWorkloadConfigInfo;

public class SchedulerUtil {
    public static IWorkloadConfigInfo fetchSchedulerConfigDescriptor(MetadataProvider metadataProvider)
            throws AlgebricksException {
        SchedulerConfigMetadataEntity scme = metadataProvider.findEnabledSchedulerConfig();
        if (scme == null) {
            return null;
        }
        SchedulerConfigRecordDescriptor scrd = (SchedulerConfigRecordDescriptor) scme.getSchedulerConfig();
        if (scrd.getName().equals(SCHEDULER_DEFAULT_CONFIG_NAME)) {
            return null;
        }
        return new EnableConfigInfo(scrd.getDefaultPriority(), scrd.getShortMemoryPercent(),
                (int) scrd.getShortCPUQuota(), scrd.getGroupToPriority());
    }
}
