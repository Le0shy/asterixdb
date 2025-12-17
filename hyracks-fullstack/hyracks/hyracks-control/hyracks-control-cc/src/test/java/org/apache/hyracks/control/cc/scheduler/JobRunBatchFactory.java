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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.job.JobRun;

public class JobRunBatchFactory {
    private static final Random random = new Random(0);

    public static List<JobRun> createJobBatch(int count) {
        List<JobRun> jobRuns = new ArrayList<>();
        int n = random.nextInt(0, 10);
        JobSpecification shortJS = TestUtils.mockJobSpecification(4, 0);
        JobSpecification prioritizedJS = TestUtils.mockJobSpecification(4, 5);

        for (int i = 0; i < count; i++) {
            long id = i + 1;
            JobRun jobRun;
            if (n < 2) {
                jobRun = generateShortJobWithExecutionTime(id, shortJS);
            } else if (n < 4) {
                jobRun = generateDefaultJob(id);
            } else {
                jobRun = generatePrioritizedJobWithExecutionTime(id, prioritizedJS);
            }

            jobRuns.add(jobRun);
        }
        return jobRuns;
    }

    private static JobRun generateShortJobWithExecutionTime(long id, JobSpecification js) {
        long executionTime = random.nextInt(0, 5);
        return JobRunFactory.mockShortJobWithExecutionTime(id, executionTime, js);
    }

    private static JobRun generateDefaultJob(long id) {
        int n = random.nextInt(1, 11);
        long executionTime = 5 + (n - 1) * 50L;
        long mems = (n - 1) * 10L;
        JobSpecification js = TestUtils.mockJobSpecification(4, mems);
        return JobRunFactory.mockDefaultJob(id, executionTime, js);
    }

    private static JobRun generatePrioritizedJobWithExecutionTime(long id, JobSpecification js) {
        long executionTime = random.nextInt(50, 1000);
        int priority = random.nextInt(1, 11); // Priority between 1 and 10
        return JobRunFactory.mockPrioritizedJobWithExecutionTime(id, executionTime, priority, js);
    }

}
