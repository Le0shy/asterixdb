package org.apache.asterix.metadata.entities;

import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.runtime.scheduler.SchedulerConfigDescriptor;

public class SchedulerConfigMetadataEntity implements IMetadataEntity<SchedulerConfigMetadataEntity> {
    private static final long serialVersionUID = -8257829613982301855L;

    private final SchedulerConfigDescriptor schedulerConfig;

    public SchedulerConfigMetadataEntity(SchedulerConfigDescriptor config) {
        this.schedulerConfig = config;
    }

    public SchedulerConfigDescriptor getSchedulerConfig() {
        return schedulerConfig;
    }

    @Override
    public SchedulerConfigMetadataEntity addToCache(MetadataCache cache) {
        return cache.addSchedulerConfigIfNotExists(this);
    }

    @Override
    public SchedulerConfigMetadataEntity dropFromCache(MetadataCache cache) {
        return cache.dropSchedulerConfig(this);
    }

}
