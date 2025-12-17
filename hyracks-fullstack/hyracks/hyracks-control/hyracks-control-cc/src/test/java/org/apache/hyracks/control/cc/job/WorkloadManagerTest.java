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

package org.apache.hyracks.control.cc.job;

import static org.apache.hyracks.control.cc.scheduler.TestUtils.*;

import java.io.IOException;
import java.util.List;

import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.scheduler.JobRunBatchFactory;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.junit.Before;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;

public class WorkloadManagerTest {
    private CCConfig ccConfig;

    @Before
    public void setup() throws IOException, CmdLineException {
        ccConfig = new CCConfig();
        ccConfig.getConfigManager().processConfig();
    }

    @Test
    public void testJobQueueProcessing() throws IOException, CmdLineException {
        IJobCapacityController jobCapacityController = mockJobCapacityController(66, 100L);
        IJobManager jobManager = mockJobManager(jobCapacityController, ccConfig);

        List<JobRun> jobs = JobRunBatchFactory.createJobBatch(1000);
        for (JobRun job : jobs) {
            jobManager.add(job);
        }
    }

}
