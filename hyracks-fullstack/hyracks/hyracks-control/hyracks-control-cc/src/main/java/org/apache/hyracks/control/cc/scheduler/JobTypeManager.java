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
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobTypeManager implements IJobTypeManager {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final String GROUP_LONG_JOB = "l";
    private static final String GROUP_SHORT_JOB = "s";
    private Map<String, Long> queryGroupToPriority;
    private long defaultPriority = 4;

    public enum JobSchedulingType {
        SHORT,
        LONG,
        NORMAL,
        DEFAULT
    }

    /* TODO: get config while system bootstraps
       fetching from metadata provider or otherwise? */
    public JobTypeManager() {
        queryGroupToPriority = new HashMap<>();
        queryGroupToPriority.put("g0", 1L);
        queryGroupToPriority.put("g1", 2L);
        queryGroupToPriority.put("g2", 4L);
        queryGroupToPriority.put("g3", 8L);
        queryGroupToPriority.put("g4", 16L);
        queryGroupToPriority.put("g5", 32L);
    }

    @Override
    public void setJobType(JobRun jobRun) {
        JobSpecification job = jobRun.getJobSpecification();
        String groupName = job.getGroupName();
        if (groupName == null) {
            jobRun.setSchedulingType(JobSchedulingType.DEFAULT);
        } else if (queryGroupToPriority.containsKey(groupName)) {
            jobRun.setSchedulingType(JobSchedulingType.NORMAL);
            long priority = queryGroupToPriority.get(groupName);
            jobRun.setPriority((int) priority);
        } else if (groupName.equals(GROUP_LONG_JOB)) {
            jobRun.setSchedulingType(JobSchedulingType.LONG);
        } else if (groupName.equals(GROUP_SHORT_JOB)) {
            jobRun.setSchedulingType(JobSchedulingType.SHORT);
        } else {
            jobRun.setSchedulingType(JobSchedulingType.DEFAULT);
        }

        LOGGER.log(Level.DEBUG, "JobID:{}, JobType:{}", jobRun.getJobId(), jobRun.getSchedulingType());
    }

    @Override
    public void setDefaultPriority(long dp) {
        defaultPriority = dp;
    }

    @Override
    public void setWorkloadConfig(Map<String, Long> groupsToPriorities) {
        queryGroupToPriority = groupsToPriorities;
    }

    @Override
    public void addGroups(Map<String, Long> groupsToAdd) {
        for (String groupName : groupsToAdd.keySet()) {
            queryGroupToPriority.put(groupName, groupsToAdd.get(groupName));
        }
    }

    @Override
    public void removeGroups(List<String> groupsToRemove) {
        for (String groupName : groupsToRemove) {
            queryGroupToPriority.remove(groupName);
        }
    }

    @Override
    public long getDefaultPriority() {
        return defaultPriority;
    }
}
