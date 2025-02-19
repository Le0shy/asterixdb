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
package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class EmptyDiskComponentMetadata extends DiskComponentMetadata {
    public static final EmptyDiskComponentMetadata INSTANCE = new EmptyDiskComponentMetadata();

    private EmptyDiskComponentMetadata() {
        super(null);
    }

    @Override
    public void put(IValueReference key, IValueReference value) throws HyracksDataException {
        // No op
    }

    @Override
    public boolean get(IValueReference key, ArrayBackedValueStorage storage) throws HyracksDataException {
        throw new IllegalStateException("Attempt to read metadata of empty component");
    }

    @Override
    public void put(MemoryComponentMetadata metadata) throws HyracksDataException {
        // No op
    }
}
