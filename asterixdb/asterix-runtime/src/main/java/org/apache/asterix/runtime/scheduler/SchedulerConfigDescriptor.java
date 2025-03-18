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

package org.apache.asterix.runtime.scheduler;

import org.apache.asterix.common.metadata.DataverseName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchedulerConfigDescriptor implements ISchedulerConfigDescriptor {
    private static final long serialVersionUID = 3L;

    private final String databaseName;
    private final DataverseName dataverseName;
    private final String name;
    private final long defaultPriority;
    private final double shortMemoryPercent;
    private final long shortCPUQuota;
    private final Map<Long, List<String>> priorityToGroups;
    private final Map<String, Long> groupToPriority;
    public SchedulerConfigDescriptor(String databaseName, DataverseName dataverseName, String name,
             long defaultPriority, double shortMemoryPercent, long shortCPUQuota,
            Map<Long, List<String>> priorityToGroups) {
        //TODO(DB): database name should not be null
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.name = name;
        this.defaultPriority = defaultPriority;
        this.shortMemoryPercent = shortMemoryPercent;
        this.shortCPUQuota = shortCPUQuota;
        this.priorityToGroups = priorityToGroups;
        this.groupToPriority = createMapping(this.priorityToGroups);
    }

    public SchedulerConfigDescriptor(String databaseName, DataverseName dataverseName, String name,
            long defaultPriority, double shortMemoryPercent, long shortCPUQuota,
            Map<String, Long> groupToPriority, boolean bound) {
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.name = name;
        this.defaultPriority = defaultPriority;
        this.shortMemoryPercent = shortMemoryPercent;
        this.shortCPUQuota = shortCPUQuota;
        this.groupToPriority = groupToPriority;
        this.priorityToGroups = createMapping(groupToPriority, true);
    }

    private Map<String, Long> createMapping(Map<Long, List<String>> priorityToGroup) {
        if(priorityToGroup == null) {
            return null;
        }
        Map<String, Long> groupToPriority = new HashMap<>();
        for(long priority: priorityToGroup.keySet()) {
            List<String> groups = priorityToGroups.get(priority);
            for (String group: groups) {
                groupToPriority.put(group, priority);
            }
        }

        return groupToPriority;
    }

    private Map<Long, List<String>> createMapping(Map<String, Long> groupToPriority, boolean bound) {
        Map<Long, List<String>> priorityToGroups = new HashMap<>();
        if (groupToPriority == null) {
            return null;
        }
        for(String group: groupToPriority.keySet()) {
            long priority = groupToPriority.get(group);
            priorityToGroups.putIfAbsent(priority, new ArrayList<>());
            priorityToGroups.get(priority).add(group);
        }
        return priorityToGroups;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getDefaultPriority() {
        return defaultPriority;
    }

    @Override
    public double getShortMemoryPercent(){
        return shortMemoryPercent;
    }

    @Override
    public long getShortCPUQuota() {
        return shortCPUQuota;
    }

    @Override
    public Map<Long, List<String>> getPriorityToGroup() {
        return priorityToGroups;
    }

    @Override
    public Map<String, Long> getGroupToPriority() {
        return groupToPriority;
    }
}