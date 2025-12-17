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

import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.job.JobRun;

public class JobRunFactory {
    public static JobRun mockPrioritizedJob(long id, int priority, JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.NORMAL).withPriority(priority)
                .withJobSpecification(js).build();
    }

    public static JobRun mockPrioritizedJobWithExecutionTime(long id, long executionTime, int priority,
            JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.NORMAL).withPriority(priority)
                .withExecutionTime(executionTime).withJobSpecification(js).build();
    }

    public static JobRun mockDefaultJob(long id, long executionTime, JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.DEFAULT)
                .withExecutionTime(executionTime).withJobSpecification(js).build();
    }

    public static JobRun mockShortJob(long id, JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.SHORT).withJobSpecification(js)
                .build();
    }

    public static JobRun mockShortJobWithExecutionTime(long id, long executionTime, JobSpecification js) {
        return new JobRunBuilder(id).withSchedulingType(JobTypeManager.JobSchedulingType.SHORT)
                .withExecutionTime(executionTime).withJobSpecification(js).build();
    }
}
