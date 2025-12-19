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
