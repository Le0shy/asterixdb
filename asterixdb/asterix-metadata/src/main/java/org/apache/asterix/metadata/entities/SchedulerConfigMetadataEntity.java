/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.metadata.entities;

import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.runtime.scheduler.ISchedulerConfigDescriptor;
import org.apache.asterix.runtime.scheduler.SchedulerConfigRecordDescriptor;
import org.apache.asterix.runtime.scheduler.SchedulerConfigStateDescriptor;

public class SchedulerConfigMetadataEntity implements IMetadataEntity<SchedulerConfigMetadataEntity> {
    public static final String SCHEDULER_DEFAULT_CONFIG_NAME = "d";
    public static final String SCHEDULER_STATE = "s";
    public static final int SCHEDULER_CONFIG_MINIMUM_LENGTH = 2;
    private static final long serialVersionUID = -8257829613982301855L;

    private final ISchedulerConfigDescriptor schedulerConfig;

    public SchedulerConfigMetadataEntity(ISchedulerConfigDescriptor config) {
        this.schedulerConfig = config;
    }

    public ISchedulerConfigDescriptor getSchedulerConfig() {
        return schedulerConfig;
    }

    @Override
    public SchedulerConfigMetadataEntity addToCache(MetadataCache cache) {
        return cache.addSchedulerConfigIfNotExists(this);
    }

    @Override
    public SchedulerConfigMetadataEntity dropFromCache(MetadataCache cache) {
        return cache.dropSchedulerConfig(schedulerConfig.getName());
    }

    public void upsertQueryGroup(Map<String, Long> upsertQueryGroups) {
        SchedulerConfigRecordDescriptor scrd = (SchedulerConfigRecordDescriptor) schedulerConfig;
        scrd.upsertQueryGroup(upsertQueryGroups);
    }

    public boolean deleteQueryGroup(List<String> deleteQueryGroups) {
        SchedulerConfigRecordDescriptor scrd = (SchedulerConfigRecordDescriptor) schedulerConfig;
        return scrd.deleteQueryGroup(deleteQueryGroups);
    }

    public void setEnabled(String setEnabledConfigName) {
        SchedulerConfigStateDescriptor scsd = (SchedulerConfigStateDescriptor) schedulerConfig;
        scsd.setEnabledConfigName(setEnabledConfigName);
    }

    public void updateConfigParameters(long defaultPriority, double shortMemoryPercent, long shortCPUQuota) {
        SchedulerConfigRecordDescriptor scrd = (SchedulerConfigRecordDescriptor) schedulerConfig;
        scrd.setDefaultPriority(defaultPriority);
        scrd.setShortMemoryPercent(shortMemoryPercent);
        scrd.setShortCPUQuota(shortCPUQuota);
    }

    public String getEnabled() {
        SchedulerConfigStateDescriptor scsd = (SchedulerConfigStateDescriptor) schedulerConfig;
        return scsd.getEnabledConfigName();
    }
}
