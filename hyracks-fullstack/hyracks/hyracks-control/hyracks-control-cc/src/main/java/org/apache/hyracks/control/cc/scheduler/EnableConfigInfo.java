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

package org.apache.hyracks.control.cc.scheduler;

import java.util.HashMap;
import java.util.Map;

public class EnableConfigInfo implements IWorkloadConfigInfo {
    private long defaultPriority;
    private double shortMemoryPercent;
    private int shortCPUQuota;
    private Map<String, Long> groupsToPriorities;
    private IWorkloadConfigInfo.Type type;

    public EnableConfigInfo(long defaultPriority, double shortMemoryPercent, int shortCPUQuota,
            Map<String, Long> groupToPriorities) {
        this.defaultPriority = defaultPriority;
        this.shortMemoryPercent = shortMemoryPercent;
        this.shortCPUQuota = shortCPUQuota;
        this.groupsToPriorities = groupToPriorities;
        this.type = IWorkloadConfigInfo.Type.ENABLE_CONFIG;
    }

    public long getDefaultPriority() {
        return defaultPriority;
    }

    public void setDefaultPriority(long defaultPriority) {
        this.defaultPriority = defaultPriority;
    }

    public double getShortMemoryPercent() {
        return shortMemoryPercent;
    }

    public void setShortMemoryPercent(double shortMemoryPercent) {
        this.shortMemoryPercent = shortMemoryPercent;
    }

    public int getShortCPUQuota() {
        return shortCPUQuota;
    }

    public void setShortCPUQuota(int shortCPUQuota) {
        this.shortCPUQuota = shortCPUQuota;
    }

    public Map<String, Long> getGroupsToPriorities() {
        return groupsToPriorities;
    }

    public IWorkloadConfigInfo.Type getType() {
        return type;
    }

    public void setGroupsToPriorities(HashMap<String, Long> groupsToPriorities) {
        this.groupsToPriorities = groupsToPriorities;
    }
}
