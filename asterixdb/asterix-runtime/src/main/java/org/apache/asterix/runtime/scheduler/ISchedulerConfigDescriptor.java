package org.apache.asterix.runtime.scheduler;

import org.apache.asterix.common.metadata.DataverseName;

public interface ISchedulerConfigDescriptor {
    String getName();

    String getDatabaseName();

    DataverseName getDataverseName();
}
