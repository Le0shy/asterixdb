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
package org.apache.hyracks.util;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
public class ThreadStats implements IThreadStats {

    private final AtomicLong pinnedPagesCount = new AtomicLong();
    private final AtomicLong coldReadCount = new AtomicLong();
    private final AtomicLong cloudReadRequestCount = new AtomicLong();
    private final AtomicLong cloudReadPageCount = new AtomicLong();
    private final AtomicLong cloudPersistPageCount = new AtomicLong();

    @Override
    public void pagePinned() {
        pinnedPagesCount.incrementAndGet();
    }

    @Override
    public long getPinnedPagesCount() {
        return pinnedPagesCount.get();
    }

    @Override
    public long getColdReadCount() {
        return coldReadCount.get();
    }

    @Override
    public void coldRead() {
        coldReadCount.incrementAndGet();
    }

    @Override
    public void cloudReadRequest() {
        cloudReadRequestCount.incrementAndGet();
    }

    @Override
    public long getCloudReadRequestCount() {
        return cloudReadRequestCount.get();
    }

    @Override
    public void cloudPageRead() {
        cloudReadPageCount.incrementAndGet();
    }

    @Override
    public long getCloudPageReadCount() {
        return cloudReadPageCount.get();
    }

    @Override
    public void cloudPagePersist() {
        cloudPersistPageCount.incrementAndGet();
    }

    @Override
    public long getCloudPagePersistCount() {
        return cloudPersistPageCount.get();
    }
}
