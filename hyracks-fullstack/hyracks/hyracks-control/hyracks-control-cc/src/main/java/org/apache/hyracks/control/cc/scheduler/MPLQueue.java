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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.job.JobRun;

public class MPLQueue {
    private final Map<JobId, JobRun> jobs = new LinkedHashMap<>();
    private Iterator<Map.Entry<JobId, JobRun>> it;
    private int id;
    private int jobQueueCapacity;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("queue_id: " + id + ",\n");
        sb.append("jobs:{ ");
        for (JobId jid : jobs.keySet()) {
            sb.append(jid + ",");
        }
        sb.append("}");
        return sb.toString();
    }

    public MPLQueue(int id, int jobQueueCapacity) {
        this.id = id;
        this.jobQueueCapacity = jobQueueCapacity;
    }

    public int getQueueSize() {
        return jobs.size();
    }

    public void put(JobId id, JobRun run) throws HyracksException {
        if (getQueueSize() >= jobQueueCapacity) {
            throw HyracksException.create(ErrorCode.JOB_QUEUE_FULL, jobQueueCapacity);
        }
        this.jobs.put(id, run);
    }

    public JobRun remove(JobId id) {
        return jobs.remove(id);
    }

    public JobRun get(JobId id) {
        return jobs.get(id);
    }

    public JobRun getFirst() {
        it = jobs.entrySet().iterator();
        if (it.hasNext()) {
            return it.next().getValue();
        }
        return null;
    }

    public int getId() {
        return id;
    }
}
