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

package org.apache.hyracks.storage.am.vector.util;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;

/**
 * Test context for VectorClusteringTree tests, providing a mock implementation
 * until the actual VectorClusteringTree is implemented.
 */
@SuppressWarnings("rawtypes")
public class VectorTreeTestContext extends AbstractVectorTreeTestContext {

    public VectorTreeTestContext(ISerializerDeserializer[] fieldSerdes, IIndex index, boolean filtered,
            int vectorDimensions) throws HyracksDataException {
        super(fieldSerdes, index, filtered, vectorDimensions);
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        // Return empty array for now - vector comparators would be needed
        return new IBinaryComparatorFactory[0];
    }

    @Override
    public int getKeyFieldCount() {
        // For vectors, typically 1 key field (the vector itself)
        return 1;
    }

    public static VectorTreeTestContext create(IIOManager ioManager, IBufferCache virtualBufferCache,
            FileReference fileRef, IBufferCache diskBufferCache, ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType, int vectorDimensions) throws Exception {

        // Create type traits for the fields
        ITypeTraits[] typeTraits = new ITypeTraits[fieldSerdes.length];
        for (int i = 0; i < fieldSerdes.length; i++) {
            if (fieldSerdes[i] instanceof FloatArraySerializerDeserializer) {
                // Vector field - for now use a simple type trait
                typeTraits[i] = new ITypeTraits() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean isFixedLength() {
                        return false; // Vectors can be variable length
                    }

                    @Override
                    public int getFixedLength() {
                        return -1; // Variable length
                    }
                };
            } else if (fieldSerdes[i] instanceof UTF8StringSerializerDeserializer) {
                // String field
                typeTraits[i] = new ITypeTraits() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean isFixedLength() {
                        return false;
                    }

                    @Override
                    public int getFixedLength() {
                        return -1;
                    }
                };
            }
        }

        // For now, create a mock index that implements the basic IIndex interface
        // TODO: Replace with actual VectorClusteringTree once implemented
        IIndex mockIndex = new MockVectorIndex(fileRef);

        return new VectorTreeTestContext(fieldSerdes, mockIndex, false, vectorDimensions);
    }

    // Mock implementation of IIndex for testing purposes
    private static class MockVectorIndex implements IIndex {
        @SuppressWarnings("unused")
        private final FileReference fileRef;
        @SuppressWarnings("unused")
        private boolean activated = false;
        @SuppressWarnings("unused")
        private boolean created = false;

        public MockVectorIndex(FileReference fileRef) {
            this.fileRef = fileRef;
        }

        @Override
        public void create() throws HyracksDataException {
            created = true;
        }

        @Override
        public void activate() throws HyracksDataException {
            activated = true;
        }

        @Override
        public void deactivate() throws HyracksDataException {
            activated = false;
        }

        @Override
        public void destroy() throws HyracksDataException {
            created = false;
            activated = false;
        }

        @Override
        public void clear() throws HyracksDataException {
            // Mock implementation
        }

        @Override
        public IIndexAccessor createAccessor(IIndexAccessParameters iap) throws HyracksDataException {
            return new MockVectorIndexAccessor();
        }

        @Override
        public void validate() throws HyracksDataException {
            // Mock validation - always passes
        }

        @Override
        public IBufferCache getBufferCache() {
            return null; // Mock implementation
        }

        @Override
        public int getNumOfFilterFields() {
            return 0;
        }

        @Override
        public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
            return null; // Mock bulk loader
        }

        @Override
        public void purge() throws HyracksDataException {
            // Mock purge
        }

        // Mock IndexAccessor
        private static class MockVectorIndexAccessor implements IIndexAccessor {

            @Override
            public void insert(org.apache.hyracks.dataflow.common.data.accessors.ITupleReference tuple)
                    throws HyracksDataException {
                // Mock insertion - just log that it happened
            }

            @Override
            public void update(org.apache.hyracks.dataflow.common.data.accessors.ITupleReference tuple)
                    throws HyracksDataException {
                // Mock update
            }

            @Override
            public void delete(org.apache.hyracks.dataflow.common.data.accessors.ITupleReference tuple)
                    throws HyracksDataException {
                // Mock delete
            }

            @Override
            public void upsert(org.apache.hyracks.dataflow.common.data.accessors.ITupleReference tuple)
                    throws HyracksDataException {
                // Mock upsert
            }

            @Override
            public IIndexCursor createSearchCursor(boolean exclusive) {
                return new MockVectorIndexCursor();
            }

            @Override
            public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
                // Mock search
            }

            @Override
            public void destroy() throws HyracksDataException {
                // Mock destroy
            }
        }

        // Mock IndexCursor
        private static class MockVectorIndexCursor implements IIndexCursor {

            @Override
            public void open(ICursorInitialState initialState, ISearchPredicate searchPred)
                    throws HyracksDataException {
                // Mock open
            }

            @Override
            public boolean hasNext() throws HyracksDataException {
                return false; // Mock - no results
            }

            @Override
            public void next() throws HyracksDataException {
                // Mock next
            }

            @Override
            public org.apache.hyracks.dataflow.common.data.accessors.ITupleReference getTuple() {
                return null; // Mock - no tuple
            }

            @Override
            public void close() throws HyracksDataException {
                // Mock close
            }

            @Override
            public void destroy() throws HyracksDataException {
                // Mock destroy
            }
        }
    }

}
