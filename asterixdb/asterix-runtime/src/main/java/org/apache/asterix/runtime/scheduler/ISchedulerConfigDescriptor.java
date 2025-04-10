package org.apache.asterix.runtime.scheduler;

import org.apache.asterix.common.metadata.DataverseName;

import java.util.List;
import java.util.Map;

public interface ISchedulerConfigDescriptor {
    String getName();
    String getDatabaseName();
    DataverseName getDataverseName();
}
