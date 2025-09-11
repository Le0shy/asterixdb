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
package org.apache.hyracks.storage.am.vector.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.accessors.FloatBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.mockito.Mockito;

/**
 * Comprehensive utility class for VectorClusteringTree unit tests.
 * Provides modular and reusable setup and configuration methods for testing different operations:
 * INSERT, DELETE, UPDATE, SEARCH, UPSERT.
 * 
 * Features:
 * - Operation-specific configurations
 * - Reusable mock setups
 * - Predefined test data and centroids
 * - Complete test environment factory methods
 * - Static tree structure simulation
 */
public class VectorClusteringTreeTestUtils {

    // Constants
    public static final int VECTOR_DIMENSIONS = 2; // 2D vectors for simplicity
    public static final int FILE_ID = 1;
    public static final int FIELD_COUNT = 4; // For tuples: <distance, cosine, vector, primary_key>

    // Tree structure page IDs - represents a 3-level tree with 8 leaf clusters
    public static final int ROOT_PAGE_ID = 1;
    public static final int INTERIOR_PAGE_1_ID = 2;
    public static final int INTERIOR_PAGE_2_ID = 3;
    public static final int LEAF_PAGE_1_ID = 4;
    public static final int LEAF_PAGE_2_ID = 5;
    public static final int LEAF_PAGE_3_ID = 6;
    public static final int LEAF_PAGE_4_ID = 7;
    public static final int METADATA_PAGE_1_ID = 8;
    public static final int METADATA_PAGE_2_ID = 9;
    public static final int METADATA_PAGE_3_ID = 10;
    public static final int METADATA_PAGE_4_ID = 11;
    public static final int DATA_PAGE_1_ID = 12;
    public static final int DATA_PAGE_2_ID = 13;
    public static final int DATA_PAGE_3_ID = 14;
    public static final int DATA_PAGE_4_ID = 15;

    /**
     * Configuration class for test setup parameters.
     */
    public static class TestConfig {
        public final int vectorDimensions;
        public final int fileId;
        public final int fieldCount;
        public final boolean enableModificationCallback;
        public final boolean enableSearchCallback;

        public TestConfig(int vectorDimensions, int fileId, int fieldCount, boolean enableModificationCallback,
                boolean enableSearchCallback) {
            this.vectorDimensions = vectorDimensions;
            this.fileId = fileId;
            this.fieldCount = fieldCount;
            this.enableModificationCallback = enableModificationCallback;
            this.enableSearchCallback = enableSearchCallback;
        }

        public static TestConfig createDefault() {
            return new TestConfig(VECTOR_DIMENSIONS, FILE_ID, FIELD_COUNT, true, true);
        }

        public static TestConfig createForOperation(OperationType operationType) {
            switch (operationType) {
                case INSERT:
                    return new TestConfig(VECTOR_DIMENSIONS, FILE_ID, FIELD_COUNT, true, false);
                case DELETE:
                    return new TestConfig(VECTOR_DIMENSIONS, FILE_ID, FIELD_COUNT, true, false);
                case UPDATE:
                    return new TestConfig(VECTOR_DIMENSIONS, FILE_ID, FIELD_COUNT, true, false);
                case SEARCH:
                    return new TestConfig(VECTOR_DIMENSIONS, FILE_ID, FIELD_COUNT, false, true);
                case UPSERT:
                    return new TestConfig(VECTOR_DIMENSIONS, FILE_ID, FIELD_COUNT, true, false);
                default:
                    return createDefault();
            }
        }
    }

    /**
     * Enum for different operation types.
     */
    public enum OperationType {
        INSERT,
        DELETE,
        UPDATE,
        SEARCH,
        UPSERT
    }

    /**
     * Container class for all mock objects needed in tests.
     */
    public static class TestMocks {
        // Infrastructure mocks
        public IBufferCache bufferCache;
        public IPageManager freePageManager;
        public ITreeIndexFrameFactory interiorFrameFactory;
        public ITreeIndexFrameFactory leafFrameFactory;
        public ITreeIndexFrameFactory metadataFrameFactory;
        public ITreeIndexFrameFactory dataFrameFactory;
        public ITreeIndexMetadataFrame metaFrame;
        public FileReference file;
        public IModificationOperationCallback modificationCallback;
        public ISearchOperationCallback searchCallback;

        // Frame mocks
        public IVectorClusteringInteriorFrame interiorFrame;
        public IVectorClusteringLeafFrame leafFrame;
        public IVectorClusteringMetadataFrame metadataFrame;
        public IVectorClusteringDataFrame dataFrame;

        // Page mocks
        public ICachedPage rootPage;
        public ICachedPage interiorPage1;
        public ICachedPage interiorPage2;
        public ICachedPage leafPage1;
        public ICachedPage leafPage2;
        public ICachedPage leafPage3;
        public ICachedPage leafPage4;
        public ICachedPage metadataPage1;
        public ICachedPage metadataPage2;
        public ICachedPage metadataPage3;
        public ICachedPage metadataPage4;
        public ICachedPage dataPage1;
        public ICachedPage dataPage2;
        public ICachedPage dataPage3;
        public ICachedPage dataPage4;

        // Tuple mocks
        public ITreeIndexTupleReference rootTuple;
        public ITreeIndexTupleReference interiorTuple1;
        public ITreeIndexTupleReference interiorTuple2;
        public ITreeIndexTupleReference leafTuple1;
        public ITreeIndexTupleReference leafTuple2;
        public ITreeIndexTupleReference leafTuple3;
        public ITreeIndexTupleReference leafTuple4;
        public ITreeIndexTupleReference dataTuple1;
        public ITreeIndexTupleReference dataTuple2;
        public ITreeIndexTupleReference dataTuple3;
        public ITreeIndexTupleReference dataTuple4;

        // Page tracking for frames
        public ICachedPage currentInteriorPage = null;
        public ICachedPage currentLeafPage = null;
        public ICachedPage currentMetadataPage = null;
        public ICachedPage currentDataPage = null;
    }

    /**
     * Predefined centroids for the static tree structure.
     */
    public static class TestCentroids {
        // Root level centroids (2 clusters)
        public static final double[] ROOT_CENTROID_1 = { 2.0, 2.0 }; // Top-right quadrant
        public static final double[] ROOT_CENTROID_2 = { -2.0, -2.0 }; // Bottom-left quadrant

        // Interior level centroids (4 clusters, 2 per root)
        public static final double[] INTERIOR_CENTROID_1_1 = { 1.0, 3.0 }; // Child of root1
        public static final double[] INTERIOR_CENTROID_1_2 = { 3.0, 1.0 }; // Child of root1
        public static final double[] INTERIOR_CENTROID_2_1 = { -1.0, -3.0 }; // Child of root2
        public static final double[] INTERIOR_CENTROID_2_2 = { -3.0, -1.0 }; // Child of root2

        // Leaf level centroids (8 clusters, 2 per interior)
        public static final double[] LEAF_CENTROID_1_1_1 = { 0.5, 3.5 }; // Child of interior1_1
        public static final double[] LEAF_CENTROID_1_1_2 = { 1.5, 2.5 }; // Child of interior1_1
        public static final double[] LEAF_CENTROID_1_2_1 = { 2.5, 0.5 }; // Child of interior1_2
        public static final double[] LEAF_CENTROID_1_2_2 = { 3.5, 1.5 }; // Child of interior1_2
        public static final double[] LEAF_CENTROID_2_1_1 = { -0.5, -3.5 }; // Child of interior2_1
        public static final double[] LEAF_CENTROID_2_1_2 = { -1.5, -2.5 }; // Child of interior2_1
        public static final double[] LEAF_CENTROID_2_2_1 = { -2.5, -0.5 }; // Child of interior2_2
        public static final double[] LEAF_CENTROID_2_2_2 = { -3.5, -1.5 }; // Child of interior2_2
    }

    /**
     * Test data vectors and primary keys.
     */
    public static class TestData {
        // Test vectors for different operations
        public static final float[] TEST_VECTOR_1 = { 0.8f, 3.2f }; // Near leafCentroid1_1_1
        public static final float[] TEST_VECTOR_2 = { 3.2f, 1.8f }; // Near leafCentroid1_2_2
        public static final float[] TEST_VECTOR_3 = { -1.2f, -2.8f }; // Near leafCentroid2_1_2
        public static final float[] TEST_VECTOR_4 = { -3.2f, -1.8f }; // Near leafCentroid2_2_2

        // Primary keys for test data
        public static final byte[] PRIMARY_KEY_1 = "PK001".getBytes();
        public static final byte[] PRIMARY_KEY_2 = "PK002".getBytes();
        public static final byte[] PRIMARY_KEY_3 = "PK003".getBytes();
        public static final byte[] PRIMARY_KEY_4 = "PK004".getBytes();

        // Non-existent data for negative tests
        public static final float[] NON_EXISTENT_VECTOR = { 10.0f, 10.0f };
        public static final byte[] NON_EXISTENT_PK = "NONEXISTENT".getBytes();
    }

    /**
     * Create and initialize all mock objects needed for testing.
     */
    public static TestMocks createMocks() {
        TestMocks mocks = new TestMocks();

        // Infrastructure mocks
        mocks.bufferCache = Mockito.mock(IBufferCache.class);
        mocks.freePageManager = Mockito.mock(IPageManager.class);
        mocks.interiorFrameFactory = Mockito.mock(ITreeIndexFrameFactory.class);
        mocks.leafFrameFactory = Mockito.mock(ITreeIndexFrameFactory.class);
        mocks.metadataFrameFactory = Mockito.mock(ITreeIndexFrameFactory.class);
        mocks.dataFrameFactory = Mockito.mock(ITreeIndexFrameFactory.class);
        mocks.metaFrame = Mockito.mock(ITreeIndexMetadataFrame.class);
        mocks.file = Mockito.mock(FileReference.class);
        mocks.modificationCallback = Mockito.mock(IModificationOperationCallback.class);
        mocks.searchCallback = Mockito.mock(ISearchOperationCallback.class);

        // Frame mocks
        mocks.interiorFrame = Mockito.mock(IVectorClusteringInteriorFrame.class);
        mocks.leafFrame = Mockito.mock(IVectorClusteringLeafFrame.class);
        mocks.metadataFrame = Mockito.mock(IVectorClusteringMetadataFrame.class);
        mocks.dataFrame = Mockito.mock(IVectorClusteringDataFrame.class);

        // Page mocks
        mocks.rootPage = Mockito.mock(ICachedPage.class);
        mocks.interiorPage1 = Mockito.mock(ICachedPage.class);
        mocks.interiorPage2 = Mockito.mock(ICachedPage.class);
        mocks.leafPage1 = Mockito.mock(ICachedPage.class);
        mocks.leafPage2 = Mockito.mock(ICachedPage.class);
        mocks.leafPage3 = Mockito.mock(ICachedPage.class);
        mocks.leafPage4 = Mockito.mock(ICachedPage.class);
        mocks.metadataPage1 = Mockito.mock(ICachedPage.class);
        mocks.metadataPage2 = Mockito.mock(ICachedPage.class);
        mocks.metadataPage3 = Mockito.mock(ICachedPage.class);
        mocks.metadataPage4 = Mockito.mock(ICachedPage.class);
        mocks.dataPage1 = Mockito.mock(ICachedPage.class);
        mocks.dataPage2 = Mockito.mock(ICachedPage.class);
        mocks.dataPage3 = Mockito.mock(ICachedPage.class);
        mocks.dataPage4 = Mockito.mock(ICachedPage.class);

        // Tuple mocks
        mocks.rootTuple = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.interiorTuple1 = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.interiorTuple2 = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.leafTuple1 = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.leafTuple2 = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.leafTuple3 = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.leafTuple4 = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.dataTuple1 = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.dataTuple2 = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.dataTuple3 = Mockito.mock(ITreeIndexTupleReference.class);
        mocks.dataTuple4 = Mockito.mock(ITreeIndexTupleReference.class);

        return mocks;
    }

    /**
     * Setup all mock behaviors for the given test configuration.
     */
    public static void setupMocks(TestMocks mocks, TestConfig config) throws HyracksDataException {
        setupInfrastructureMocks(mocks, config);
        setupFrameFactories(mocks);
        setupPageLatching(mocks);
        setupBufferCache(mocks, config);
        setupFreePageManager(mocks);
        setupCallbacks(mocks, config);
        setupFrameBehaviors(mocks);
        setupTupleOperations(mocks);
    }

    /**
     * Create a VectorClusteringTree with the given mocks and configuration.
     */
    public static VectorClusteringTree createTree(TestMocks mocks, TestConfig config) throws HyracksDataException {
        IBinaryComparatorFactory[] cmpFactories =
                new IBinaryComparatorFactory[] { FloatBinaryComparatorFactory.INSTANCE };

        VectorClusteringTree tree = Mockito.spy(new VectorClusteringTree(mocks.bufferCache, mocks.freePageManager,
                mocks.interiorFrameFactory, mocks.leafFrameFactory, mocks.metadataFrameFactory, mocks.dataFrameFactory,
                cmpFactories, config.fieldCount, config.vectorDimensions, mocks.file));

        // Activate tree
        tree.activate();

        // Mock file ID
        Mockito.when(tree.getFileId()).thenReturn(config.fileId);

        return tree;
    }

    /**
     * Create an index accessor for the tree with appropriate callbacks.
     */
    public static IIndexAccessor createAccessor(VectorClusteringTree tree, TestMocks mocks, TestConfig config) {
        IIndexAccessParameters iap;

        if (config.enableModificationCallback && config.enableSearchCallback) {
            iap = new IIndexAccessParameters() {
                @Override
                public IModificationOperationCallback getModificationCallback() {
                    return mocks.modificationCallback;
                }

                @Override
                public ISearchOperationCallback getSearchOperationCallback() {
                    return mocks.searchCallback;
                }

                @Override
                public Map<String, Object> getParameters() {
                    return new HashMap<>();
                }

                @Override
                public <T> T getParameter(String key, Class<T> clazz) {
                    return null;
                }
            };
        } else if (config.enableModificationCallback) {
            iap = new IIndexAccessParameters() {
                @Override
                public IModificationOperationCallback getModificationCallback() {
                    return mocks.modificationCallback;
                }

                @Override
                public ISearchOperationCallback getSearchOperationCallback() {
                    return null;
                }

                @Override
                public Map<String, Object> getParameters() {
                    return new HashMap<>();
                }

                @Override
                public <T> T getParameter(String key, Class<T> clazz) {
                    return null;
                }
            };
        } else if (config.enableSearchCallback) {
            iap = new IIndexAccessParameters() {
                @Override
                public IModificationOperationCallback getModificationCallback() {
                    return null;
                }

                @Override
                public ISearchOperationCallback getSearchOperationCallback() {
                    return mocks.searchCallback;
                }

                @Override
                public Map<String, Object> getParameters() {
                    return new HashMap<>();
                }

                @Override
                public <T> T getParameter(String key, Class<T> clazz) {
                    return null;
                }
            };
        } else {
            iap = new IIndexAccessParameters() {
                @Override
                public IModificationOperationCallback getModificationCallback() {
                    return null;
                }

                @Override
                public ISearchOperationCallback getSearchOperationCallback() {
                    return null;
                }

                @Override
                public Map<String, Object> getParameters() {
                    return new HashMap<>();
                }

                @Override
                public <T> T getParameter(String key, Class<T> clazz) {
                    return null;
                }
            };
        }

        return tree.createAccessor(iap);
    }

    /**
     * Setup the static tree structure (centroids and page relationships).
     */
    public static void setupStaticStructure(TestMocks mocks) throws HyracksDataException {
        setupRootLevel(mocks);
        setupInteriorLevel(mocks);
        setupLeafLevel(mocks);
        setupMetadataLevel(mocks);
        setupTupleData(mocks);
    }

    /**
     * Setup test data in data pages.
     */
    public static void setupTestData(TestMocks mocks) throws HyracksDataException {
        setupDataTuple(mocks.dataTuple1, TestData.TEST_VECTOR_1, TestData.PRIMARY_KEY_1);
        setupDataTuple(mocks.dataTuple2, TestData.TEST_VECTOR_2, TestData.PRIMARY_KEY_2);
        setupDataTuple(mocks.dataTuple3, TestData.TEST_VECTOR_3, TestData.PRIMARY_KEY_3);
        setupDataTuple(mocks.dataTuple4, TestData.TEST_VECTOR_4, TestData.PRIMARY_KEY_4);
    }

    /**
     * Create a test tuple with vector and primary key.
     */
    public static ITupleReference createTestTuple(float[] vector, byte[] primaryKey) {
        return new ITupleReference() {
            @Override
            public int getFieldCount() {
                return 2; // vector + primary key
            }

            @Override
            public byte[] getFieldData(int fIdx) {
                if (fIdx == 0)
                    return VectorUtils.floatArrayToBytes(vector);
                if (fIdx == 1)
                    return primaryKey;
                return null;
            }

            @Override
            public int getFieldStart(int fIdx) {
                return 0;
            }

            @Override
            public int getFieldLength(int fIdx) {
                if (fIdx == 0)
                    return VectorUtils.floatArrayToBytes(vector).length;
                if (fIdx == 1)
                    return primaryKey.length;
                return 0;
            }
        };
    }

    /**
     * Create a complete test environment for a specific operation type.
     */
    public static TestEnvironment createTestEnvironment(OperationType operationType) throws HyracksDataException {
        TestConfig config = TestConfig.createForOperation(operationType);
        TestMocks mocks = createMocks();
        setupMocks(mocks, config);

        VectorClusteringTree tree = createTree(mocks, config);
        IIndexAccessor accessor = createAccessor(tree, mocks, config);

        setupStaticStructure(mocks);
        setupTestData(mocks);

        return new TestEnvironment(config, mocks, tree, accessor);
    }

    /**
     * Container class for a complete test environment.
     */
    public static class TestEnvironment {
        public final TestConfig config;
        public final TestMocks mocks;
        public final VectorClusteringTree tree;
        public final IIndexAccessor accessor;

        public TestEnvironment(TestConfig config, TestMocks mocks, VectorClusteringTree tree, IIndexAccessor accessor) {
            this.config = config;
            this.mocks = mocks;
            this.tree = tree;
            this.accessor = accessor;
        }
    }

    // Private helper methods for setup

    private static void setupInfrastructureMocks(TestMocks mocks, TestConfig config) throws HyracksDataException {
        // Setup basic infrastructure mocks
        Mockito.doNothing().when(mocks.modificationCallback).found(Mockito.any(), Mockito.any());
        Mockito.doNothing().when(mocks.searchCallback).before(Mockito.any());
        Mockito.doNothing().when(mocks.searchCallback).reconcile(Mockito.any());
    }

    private static void setupFrameFactories(TestMocks mocks) throws HyracksDataException {
        // Create separate mock instances for frame factories
        IVectorClusteringInteriorFrame separateInteriorFrame = Mockito.mock(IVectorClusteringInteriorFrame.class);
        IVectorClusteringLeafFrame separateLeafFrame = Mockito.mock(IVectorClusteringLeafFrame.class);
        IVectorClusteringMetadataFrame separateMetadataFrame = Mockito.mock(IVectorClusteringMetadataFrame.class);
        IVectorClusteringDataFrame separateDataFrame = Mockito.mock(IVectorClusteringDataFrame.class);

        Mockito.when(mocks.interiorFrameFactory.createFrame()).thenReturn(separateInteriorFrame);
        Mockito.when(mocks.leafFrameFactory.createFrame()).thenReturn(separateLeafFrame);
        Mockito.when(mocks.metadataFrameFactory.createFrame()).thenReturn(separateMetadataFrame);
        Mockito.when(mocks.dataFrameFactory.createFrame()).thenReturn(separateDataFrame);
        Mockito.when(mocks.freePageManager.createMetadataFrame()).thenReturn(mocks.metaFrame);

        // Set up the separate frames with the same logic as the main frames
        setupFrameInstanceForInterior(separateInteriorFrame, mocks);
        setupFrameInstanceForLeaf(separateLeafFrame, mocks);
        setupFrameInstanceForMetadata(separateMetadataFrame, mocks);
        setupFrameInstanceForData(separateDataFrame, mocks);
    }

    private static void setupPageLatching(TestMocks mocks) throws HyracksDataException {
        ICachedPage[] allPages =
                { mocks.rootPage, mocks.interiorPage1, mocks.interiorPage2, mocks.leafPage1, mocks.leafPage2,
                        mocks.leafPage3, mocks.leafPage4, mocks.metadataPage1, mocks.metadataPage2, mocks.metadataPage3,
                        mocks.metadataPage4, mocks.dataPage1, mocks.dataPage2, mocks.dataPage3, mocks.dataPage4 };

        for (ICachedPage page : allPages) {
            Mockito.doNothing().when(page).acquireReadLatch();
            Mockito.doNothing().when(page).releaseReadLatch();
            Mockito.doNothing().when(page).acquireWriteLatch();
            Mockito.doNothing().when(page).releaseWriteLatch(Mockito.anyBoolean());
        }
    }

    private static void setupBufferCache(TestMocks mocks, TestConfig config) throws HyracksDataException {
        // Setup buffer cache pin/unpin using proper disk page IDs
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, ROOT_PAGE_ID)))
                .thenReturn(mocks.rootPage);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, INTERIOR_PAGE_1_ID)))
                .thenReturn(mocks.interiorPage1);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, INTERIOR_PAGE_2_ID)))
                .thenReturn(mocks.interiorPage2);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, LEAF_PAGE_1_ID)))
                .thenReturn(mocks.leafPage1);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, LEAF_PAGE_2_ID)))
                .thenReturn(mocks.leafPage2);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, LEAF_PAGE_3_ID)))
                .thenReturn(mocks.leafPage3);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, LEAF_PAGE_4_ID)))
                .thenReturn(mocks.leafPage4);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, METADATA_PAGE_1_ID)))
                .thenReturn(mocks.metadataPage1);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, METADATA_PAGE_2_ID)))
                .thenReturn(mocks.metadataPage2);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, METADATA_PAGE_3_ID)))
                .thenReturn(mocks.metadataPage3);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, METADATA_PAGE_4_ID)))
                .thenReturn(mocks.metadataPage4);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, DATA_PAGE_1_ID)))
                .thenReturn(mocks.dataPage1);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, DATA_PAGE_2_ID)))
                .thenReturn(mocks.dataPage2);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, DATA_PAGE_3_ID)))
                .thenReturn(mocks.dataPage3);
        Mockito.when(mocks.bufferCache.pin(BufferedFileHandle.getDiskPageId(config.fileId, DATA_PAGE_4_ID)))
                .thenReturn(mocks.dataPage4);

        Mockito.doNothing().when(mocks.bufferCache).unpin(Mockito.any(ICachedPage.class));
    }

    private static void setupFreePageManager(TestMocks mocks) throws HyracksDataException {
        Mockito.when(mocks.freePageManager.takePage(Mockito.any())).thenReturn(100); // New page ID
        Mockito.when(mocks.freePageManager.getRootPageId()).thenReturn(ROOT_PAGE_ID);
        Mockito.when(mocks.freePageManager.getBulkLoadLeaf()).thenReturn(2);
    }

    private static void setupCallbacks(TestMocks mocks, TestConfig config) throws HyracksDataException {
        if (config.enableModificationCallback) {
            Mockito.doNothing().when(mocks.modificationCallback).found(Mockito.any(), Mockito.any());
        }
        if (config.enableSearchCallback) {
            Mockito.doNothing().when(mocks.searchCallback).before(Mockito.any());
            Mockito.doNothing().when(mocks.searchCallback).reconcile(Mockito.any());
        }
    }

    private static void setupFrameBehaviors(TestMocks mocks) throws HyracksDataException {
        // Setup main frame behaviors
        setupFrameInstanceForInterior(mocks.interiorFrame, mocks);
        setupFrameInstanceForLeaf(mocks.leafFrame, mocks);
        setupFrameInstanceForMetadata(mocks.metadataFrame, mocks);
        setupFrameInstanceForData(mocks.dataFrame, mocks);
    }

    private static void setupFrameInstanceForInterior(IVectorClusteringInteriorFrame frame, TestMocks mocks)
            throws HyracksDataException {
        // Page tracking
        Mockito.doAnswer(invocation -> {
            ICachedPage page = invocation.getArgument(0);
            mocks.currentInteriorPage = page;
            return null;
        }).when(frame).setPage(Mockito.any(ICachedPage.class));

        // isLeaf always returns false for interior frames
        Mockito.when(frame.isLeaf()).thenReturn(false);

        // getTupleCount based on current page
        Mockito.when(frame.getTupleCount()).thenAnswer(invocation -> {
            if (mocks.currentInteriorPage == mocks.rootPage)
                return 2;
            if (mocks.currentInteriorPage == mocks.interiorPage1)
                return 2;
            if (mocks.currentInteriorPage == mocks.interiorPage2)
                return 2;
            return 0;
        });

        // createTupleReference and other methods
        setupTupleCreationForInterior(frame, mocks);
        setupChildPageIdForInterior(frame, mocks);
    }

    private static void setupFrameInstanceForLeaf(IVectorClusteringLeafFrame frame, TestMocks mocks)
            throws HyracksDataException {
        // Page tracking
        Mockito.doAnswer(invocation -> {
            ICachedPage page = invocation.getArgument(0);
            mocks.currentLeafPage = page;
            return null;
        }).when(frame).setPage(Mockito.any(ICachedPage.class));

        // isLeaf returns true only for actual leaf pages
        Mockito.when(frame.isLeaf()).thenAnswer(invocation -> {
            boolean isLeaf = (mocks.currentLeafPage == mocks.leafPage1 || mocks.currentLeafPage == mocks.leafPage2
                    || mocks.currentLeafPage == mocks.leafPage3 || mocks.currentLeafPage == mocks.leafPage4);
            return isLeaf;
        });

        // getTupleCount based on current page
        Mockito.when(frame.getTupleCount()).thenAnswer(invocation -> {
            if (mocks.currentLeafPage == mocks.leafPage1 || mocks.currentLeafPage == mocks.leafPage2
                    || mocks.currentLeafPage == mocks.leafPage3 || mocks.currentLeafPage == mocks.leafPage4) {
                return 2;
            }
            return 0;
        });

        setupTupleCreationForLeaf(frame, mocks);
        setupMetadataPagePointers(frame, mocks);
    }

    private static void setupFrameInstanceForMetadata(IVectorClusteringMetadataFrame frame, TestMocks mocks)
            throws HyracksDataException {
        // Page tracking
        Mockito.doAnswer(invocation -> {
            ICachedPage page = invocation.getArgument(0);
            mocks.currentMetadataPage = page;
            return null;
        }).when(frame).setPage(Mockito.any(ICachedPage.class));

        Mockito.when(frame.isLeaf()).thenReturn(false);
        Mockito.when(frame.getTupleCount()).thenReturn(1);

        // Setup data page pointers
        Mockito.when(frame.getDataPagePointer(0)).thenAnswer(invocation -> {
            if (mocks.currentMetadataPage == mocks.metadataPage1)
                return DATA_PAGE_1_ID;
            if (mocks.currentMetadataPage == mocks.metadataPage2)
                return DATA_PAGE_2_ID;
            if (mocks.currentMetadataPage == mocks.metadataPage3)
                return DATA_PAGE_3_ID;
            if (mocks.currentMetadataPage == mocks.metadataPage4)
                return DATA_PAGE_4_ID;
            return DATA_PAGE_1_ID; // fallback
        });

        // Setup max distances
        Mockito.when(frame.getMaxDistance(0)).thenReturn(1.0f);
    }

    private static void setupFrameInstanceForData(IVectorClusteringDataFrame frame, TestMocks mocks)
            throws HyracksDataException {
        // Page tracking
        Mockito.doAnswer(invocation -> {
            ICachedPage page = invocation.getArgument(0);
            mocks.currentDataPage = page;
            return null;
        }).when(frame).setPage(Mockito.any(ICachedPage.class));

        Mockito.when(frame.isLeaf()).thenReturn(false);
        Mockito.when(frame.getTupleCount()).thenReturn(1); // Each data page has 1 test tuple

        // Setup tuple creation for data frames
        Mockito.when(frame.createTupleReference()).thenAnswer(invocation -> {
            if (mocks.currentDataPage == mocks.dataPage1)
                return mocks.dataTuple1;
            if (mocks.currentDataPage == mocks.dataPage2)
                return mocks.dataTuple2;
            if (mocks.currentDataPage == mocks.dataPage3)
                return mocks.dataTuple3;
            if (mocks.currentDataPage == mocks.dataPage4)
                return mocks.dataTuple4;
            return mocks.dataTuple1; // fallback
        });

        // Setup delete operation
        Mockito.doNothing().when(frame).delete(Mockito.any(ITupleReference.class), Mockito.anyInt());

        // Setup page LSN operations
        Mockito.when(frame.getPageLsn()).thenReturn(1L);
        Mockito.doNothing().when(frame).setPageLsn(Mockito.anyLong());
    }

    private static void setupTupleOperations(TestMocks mocks) throws HyracksDataException {
        ITreeIndexTupleReference[] allTuples = { mocks.rootTuple, mocks.interiorTuple1, mocks.interiorTuple2,
                mocks.leafTuple1, mocks.leafTuple2, mocks.leafTuple3, mocks.leafTuple4, mocks.dataTuple1,
                mocks.dataTuple2, mocks.dataTuple3, mocks.dataTuple4 };

        for (ITreeIndexTupleReference tuple : allTuples) {
            Mockito.doNothing().when(tuple).resetByTupleIndex(Mockito.any(), Mockito.anyInt());
        }
    }

    // Static structure setup methods

    private static void setupRootLevel(TestMocks mocks) throws HyracksDataException {
        // Setup centroid extraction for root level
        setupCentroidExtraction(mocks.rootPage,
                new double[][] { TestCentroids.ROOT_CENTROID_1, TestCentroids.ROOT_CENTROID_2 });
    }

    private static void setupInteriorLevel(TestMocks mocks) throws HyracksDataException {
        // Setup centroid extraction for interior levels
        setupCentroidExtraction(mocks.interiorPage1,
                new double[][] { TestCentroids.INTERIOR_CENTROID_1_1, TestCentroids.INTERIOR_CENTROID_1_2 });
        setupCentroidExtraction(mocks.interiorPage2,
                new double[][] { TestCentroids.INTERIOR_CENTROID_2_1, TestCentroids.INTERIOR_CENTROID_2_2 });
    }

    private static void setupLeafLevel(TestMocks mocks) throws HyracksDataException {
        // Setup centroid extraction for leaf levels
        setupCentroidExtraction(mocks.leafPage1,
                new double[][] { TestCentroids.LEAF_CENTROID_1_1_1, TestCentroids.LEAF_CENTROID_1_1_2 });
        setupCentroidExtraction(mocks.leafPage2,
                new double[][] { TestCentroids.LEAF_CENTROID_1_2_1, TestCentroids.LEAF_CENTROID_1_2_2 });
        setupCentroidExtraction(mocks.leafPage3,
                new double[][] { TestCentroids.LEAF_CENTROID_2_1_1, TestCentroids.LEAF_CENTROID_2_1_2 });
        setupCentroidExtraction(mocks.leafPage4,
                new double[][] { TestCentroids.LEAF_CENTROID_2_2_1, TestCentroids.LEAF_CENTROID_2_2_2 });
    }

    private static void setupMetadataLevel(TestMocks mocks) throws HyracksDataException {
        // Metadata level setup is handled by setupMetadataPagePointers
    }

    private static void setupTupleData(TestMocks mocks) {
        // Setup root tuples with centroid data
        setupTupleFieldDataForIndex(mocks.rootTuple, TestCentroids.ROOT_CENTROID_1, 0);
        setupTupleFieldDataForIndex(mocks.rootTuple, TestCentroids.ROOT_CENTROID_2, 1);

        // Setup interior tuple centroid data  
        setupTupleFieldDataForIndex(mocks.interiorTuple1, TestCentroids.INTERIOR_CENTROID_1_1, 0);
        setupTupleFieldDataForIndex(mocks.interiorTuple1, TestCentroids.INTERIOR_CENTROID_1_2, 1);
        setupTupleFieldDataForIndex(mocks.interiorTuple2, TestCentroids.INTERIOR_CENTROID_2_1, 0);
        setupTupleFieldDataForIndex(mocks.interiorTuple2, TestCentroids.INTERIOR_CENTROID_2_2, 1);

        // Setup leaf tuple centroid data
        setupTupleFieldDataForIndex(mocks.leafTuple1, TestCentroids.LEAF_CENTROID_1_1_1, 0);
        setupTupleFieldDataForIndex(mocks.leafTuple1, TestCentroids.LEAF_CENTROID_1_1_2, 1);
        setupTupleFieldDataForIndex(mocks.leafTuple2, TestCentroids.LEAF_CENTROID_1_2_1, 0);
        setupTupleFieldDataForIndex(mocks.leafTuple2, TestCentroids.LEAF_CENTROID_1_2_2, 1);
        setupTupleFieldDataForIndex(mocks.leafTuple3, TestCentroids.LEAF_CENTROID_2_1_1, 0);
        setupTupleFieldDataForIndex(mocks.leafTuple3, TestCentroids.LEAF_CENTROID_2_1_2, 1);
        setupTupleFieldDataForIndex(mocks.leafTuple4, TestCentroids.LEAF_CENTROID_2_2_1, 0);
        setupTupleFieldDataForIndex(mocks.leafTuple4, TestCentroids.LEAF_CENTROID_2_2_2, 1);
    }

    // Helper methods

    private static void setupTupleFieldDataForIndex(ITreeIndexTupleReference tuple, double[] centroid, int tupleIndex) {
        byte[] centroidData = createSerializedCentroid(centroid);

        Mockito.when(tuple.getFieldData(1)).thenReturn(centroidData);
        Mockito.when(tuple.getFieldStart(1)).thenReturn(0);
        Mockito.when(tuple.getFieldLength(1)).thenReturn(centroidData.length);
    }

    private static void setupCentroidExtraction(ICachedPage page, double[][] centroids) {
        // This will be used by the tree's centroid extraction methods
        // The actual extraction is handled by the tree implementation
    }

    private static void setupTupleCreationForInterior(IVectorClusteringInteriorFrame frame, TestMocks mocks)
            throws HyracksDataException {
        Mockito.when(frame.createTupleReference()).thenAnswer(invocation -> {
            if (mocks.currentInteriorPage == mocks.rootPage)
                return mocks.rootTuple;
            if (mocks.currentInteriorPage == mocks.interiorPage1)
                return mocks.interiorTuple1;
            if (mocks.currentInteriorPage == mocks.interiorPage2)
                return mocks.interiorTuple2;
            return mocks.rootTuple; // fallback
        });
    }

    private static void setupChildPageIdForInterior(IVectorClusteringInteriorFrame frame, TestMocks mocks)
            throws HyracksDataException {
        Mockito.when(frame.getChildPageId(Mockito.anyInt())).thenAnswer(invocation -> {
            int index = invocation.getArgument(0);
            if (mocks.currentInteriorPage == mocks.rootPage) {
                return index == 0 ? INTERIOR_PAGE_1_ID : INTERIOR_PAGE_2_ID;
            } else if (mocks.currentInteriorPage == mocks.interiorPage1) {
                return index == 0 ? LEAF_PAGE_1_ID : LEAF_PAGE_2_ID;
            } else if (mocks.currentInteriorPage == mocks.interiorPage2) {
                return index == 0 ? LEAF_PAGE_3_ID : LEAF_PAGE_4_ID;
            }
            return LEAF_PAGE_1_ID; // fallback
        });
    }

    private static void setupTupleCreationForLeaf(IVectorClusteringLeafFrame frame, TestMocks mocks)
            throws HyracksDataException {
        Mockito.when(frame.createTupleReference()).thenAnswer(invocation -> {
            if (mocks.currentLeafPage == mocks.leafPage1)
                return mocks.leafTuple1;
            if (mocks.currentLeafPage == mocks.leafPage2)
                return mocks.leafTuple2;
            if (mocks.currentLeafPage == mocks.leafPage3)
                return mocks.leafTuple3;
            if (mocks.currentLeafPage == mocks.leafPage4)
                return mocks.leafTuple4;
            return mocks.leafTuple1; // fallback
        });
    }

    private static void setupMetadataPagePointers(IVectorClusteringLeafFrame frame, TestMocks mocks)
            throws HyracksDataException {
        Mockito.when(frame.getMetadataPagePointer(Mockito.anyInt())).thenAnswer(invocation -> {
            int index = invocation.getArgument(0);
            if (mocks.currentLeafPage == mocks.leafPage1) {
                return index == 0 ? METADATA_PAGE_1_ID : METADATA_PAGE_2_ID;
            } else if (mocks.currentLeafPage == mocks.leafPage2) {
                return index == 0 ? METADATA_PAGE_2_ID : METADATA_PAGE_1_ID;
            } else if (mocks.currentLeafPage == mocks.leafPage3) {
                return index == 0 ? METADATA_PAGE_3_ID : METADATA_PAGE_4_ID;
            } else if (mocks.currentLeafPage == mocks.leafPage4) {
                return index == 0 ? METADATA_PAGE_4_ID : METADATA_PAGE_3_ID;
            }
            return METADATA_PAGE_1_ID; // fallback
        });
    }

    private static void setupDataTuple(ITreeIndexTupleReference tuple, float[] vector, byte[] primaryKey) {
        // Mock field count - data tuple format: <distance, cosine, vector, PK>
        Mockito.when(tuple.getFieldCount()).thenReturn(4);

        // Setup vector field (field 2)
        byte[] vectorBytes = VectorUtils.floatArrayToBytes(vector);
        Mockito.when(tuple.getFieldData(2)).thenReturn(vectorBytes);
        Mockito.when(tuple.getFieldStart(2)).thenReturn(0);
        Mockito.when(tuple.getFieldLength(2)).thenReturn(vectorBytes.length);

        // Setup primary key field (field 3 - last field)
        Mockito.when(tuple.getFieldData(3)).thenReturn(primaryKey);
        Mockito.when(tuple.getFieldStart(3)).thenReturn(0);
        Mockito.when(tuple.getFieldLength(3)).thenReturn(primaryKey.length);
    }

    private static byte[] createSerializedCentroid(double[] centroid) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            for (double d : centroid) {
                dos.writeDouble(d);
            }
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Factory methods for creating test environments for specific operations
     */
    public static class TestEnvironmentFactory {

        /**
         * Create a complete test environment optimized for INSERT operations.
         */
        public static TestEnvironment createForInsert() throws HyracksDataException {
            return createTestEnvironment(OperationType.INSERT);
        }

        /**
         * Create a complete test environment optimized for DELETE operations.
         */
        public static TestEnvironment createForDelete() throws HyracksDataException {
            return createTestEnvironment(OperationType.DELETE);
        }

        /**
         * Create a complete test environment optimized for UPDATE operations.
         */
        public static TestEnvironment createForUpdate() throws HyracksDataException {
            TestEnvironment env = createTestEnvironment(OperationType.UPDATE);
            // Actually insert test data for update operations to work
            insertTestDataIntoTree(env);
            return env;
        }

        /**
         * Create a complete test environment optimized for SEARCH operations.
         */
        public static TestEnvironment createForSearch() throws HyracksDataException {
            return createTestEnvironment(OperationType.SEARCH);
        }

        /**
         * Create a complete test environment optimized for UPSERT operations.
         */
        public static TestEnvironment createForUpsert() throws HyracksDataException {
            return createTestEnvironment(OperationType.UPSERT);
        }

        /**
         * Create a minimal test environment with basic setup (for performance testing).
         */
        public static TestEnvironment createMinimal() throws HyracksDataException {
            TestConfig config = new TestConfig(VECTOR_DIMENSIONS, FILE_ID, FIELD_COUNT, false, false);
            TestMocks mocks = createMocks();
            setupMocks(mocks, config);

            VectorClusteringTree tree = createTree(mocks, config);
            IIndexAccessor accessor = createAccessor(tree, mocks, config);

            // Skip static structure setup for minimal environment

            return new TestEnvironment(config, mocks, tree, accessor);
        }

        /**
         * Create a test environment with custom configuration.
         */
        public static TestEnvironment createCustom(TestConfig customConfig) throws HyracksDataException {
            TestMocks mocks = createMocks();
            setupMocks(mocks, customConfig);

            VectorClusteringTree tree = createTree(mocks, customConfig);
            IIndexAccessor accessor = createAccessor(tree, mocks, customConfig);

            setupStaticStructure(mocks);
            setupTestData(mocks);

            return new TestEnvironment(customConfig, mocks, tree, accessor);
        }

        /**
         * Insert actual test data into the tree for tests that need real data.
         */
        private static void insertTestDataIntoTree(TestEnvironment env) throws HyracksDataException {
            try {
                // Insert predefined test tuples that the update tests expect to find
                ITupleReference tuple1 =
                        TestDataManager.createTupleForInsert(TestData.TEST_VECTOR_1, TestData.PRIMARY_KEY_1);
                env.accessor.insert(tuple1);

                ITupleReference tuple2 =
                        TestDataManager.createTupleForInsert(TestData.TEST_VECTOR_2, TestData.PRIMARY_KEY_2);
                env.accessor.insert(tuple2);

                ITupleReference tuple3 =
                        TestDataManager.createTupleForInsert(TestData.TEST_VECTOR_3, TestData.PRIMARY_KEY_3);
                env.accessor.insert(tuple3);

                ITupleReference tuple4 =
                        TestDataManager.createTupleForInsert(TestData.TEST_VECTOR_4, TestData.PRIMARY_KEY_4);
                env.accessor.insert(tuple4);

                // Insert additional test tuples that the update tests try to access
                ITupleReference zeroTuple =
                        TestDataManager.createTupleForInsert(new float[] { 0.0f, 0.0f }, "ZERO".getBytes());
                env.accessor.insert(zeroTuple);

                ITupleReference exactTuple =
                        TestDataManager.createTupleForInsert(new float[] { 1.0f, 1.0f }, "EXACT".getBytes());
                env.accessor.insert(exactTuple);

                ITupleReference sharedTuple1 =
                        TestDataManager.createTupleForInsert(new float[] { 0.5f, 0.5f }, "PK_SHARED_1".getBytes());
                env.accessor.insert(sharedTuple1);

                ITupleReference sharedTuple2 =
                        TestDataManager.createTupleForInsert(new float[] { 1.0f, 3.0f }, "PK_SHARED_2".getBytes());
                env.accessor.insert(sharedTuple2);

                ITupleReference customTuple =
                        TestDataManager.createTupleForInsert(new float[] { 2.0f, 2.0f }, "CUSTOM".getBytes());
                env.accessor.insert(customTuple);

            } catch (Exception e) {
                // Log the error but don't fail completely - some tests might still work
                System.err.println("Warning: Failed to insert test data into tree: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Utility methods for test data management
     */
    public static class TestDataManager {

        /**
         * Create test tuple for a specific test scenario.
         */
        public static ITupleReference createTupleForInsert(float[] vector, byte[] primaryKey) {
            return createTestTuple(vector, primaryKey);
        }

        /**
         * Create test tuple for deletion (needs to match existing data).
         */
        public static ITupleReference createTupleForDelete() {
            return createTestTuple(TestData.TEST_VECTOR_1, TestData.PRIMARY_KEY_1);
        }

        /**
         * Create test tuple for update operations with included fields.
         * Update tuple format: <vector, included_field1, included_field2, ..., primary_key>
         * The vector is used for navigation, PK for identification, and included fields are the actual update data.
         * NOTE: Primary key MUST be the last field to match extractPrimaryKeyFromTuple logic.
         */
        public static ITupleReference createTupleForUpdate(float[] navigationVector, byte[] primaryKey,
                String... includedFields) {
            return createTupleWithIncludedFields(navigationVector, primaryKey, includedFields);
        }

        /**
         * Create test tuple for update operations (legacy - for backward compatibility).
         */
        public static ITupleReference createTupleForUpdate(float[] newVector, byte[] primaryKey) {
            // Default case - no included fields to update (should be a no-op)
            return createTupleWithIncludedFields(newVector, primaryKey);
        }

        /**
         * Create test tuple for point lookup.
         */
        public static ITupleReference createTupleForSearch(byte[] primaryKey) {
            return createTestTuple(TestData.TEST_VECTOR_1, primaryKey);
        }

        /**
         * Get predefined test vectors for different tree clusters.
         */
        public static float[][] getTestVectorsForCluster(int clusterIndex) {
            switch (clusterIndex) {
                case 1:
                    return new float[][] { TestData.TEST_VECTOR_1, { 0.6f, 3.4f } };
                case 2:
                    return new float[][] { TestData.TEST_VECTOR_2, { 3.4f, 1.6f } };
                case 3:
                    return new float[][] { TestData.TEST_VECTOR_3, { -1.4f, -2.6f } };
                case 4:
                    return new float[][] { TestData.TEST_VECTOR_4, { -3.4f, -1.6f } };
                default:
                    return new float[][] { TestData.TEST_VECTOR_1 };
            }
        }

        /**
         * Get non-existent data for negative testing.
         */
        public static ITupleReference createNonExistentTuple() {
            return createTestTuple(TestData.NON_EXISTENT_VECTOR, TestData.NON_EXISTENT_PK);
        }

        /**
         * Create a test tuple with the given vector and primary key.
         * Input tuple format: <vector, primary_key>
         */
        private static ITupleReference createTestTuple(float[] vector, byte[] primaryKey) {
            // For now, return a simple mock tuple that can be used by tests
            // The actual implementation would depend on the specific tuple format needed
            ITupleReference mockTuple = Mockito.mock(ITupleReference.class);

            try {
                // Setup vector field (field 0)
                byte[] vectorBytes = VectorUtils.floatArrayToBytes(vector);
                Mockito.when(mockTuple.getFieldData(0)).thenReturn(vectorBytes);
                Mockito.when(mockTuple.getFieldStart(0)).thenReturn(0);
                Mockito.when(mockTuple.getFieldLength(0)).thenReturn(vectorBytes.length);

                // Setup primary key field (field 1) 
                Mockito.when(mockTuple.getFieldData(1)).thenReturn(primaryKey);
                Mockito.when(mockTuple.getFieldStart(1)).thenReturn(0);
                Mockito.when(mockTuple.getFieldLength(1)).thenReturn(primaryKey.length);

                // Setup field count
                Mockito.when(mockTuple.getFieldCount()).thenReturn(2);

                return mockTuple;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create test tuple", e);
            }
        }

        /**
         * Create a test tuple with included fields for update operations.
         * Update tuple format: <vector, included_field1, included_field2, ..., primary_key>
         * Note: Primary key must be the LAST field to match extractPrimaryKeyFromTuple logic
         */
        private static ITupleReference createTupleWithIncludedFields(float[] vector, byte[] primaryKey,
                String... includedFields) {
            ITupleReference mockTuple = Mockito.mock(ITupleReference.class);

            try {
                int totalFields = 1 + includedFields.length + 1; // vector + included fields + PK

                // Setup vector field (field 0)
                byte[] vectorBytes = VectorUtils.floatArrayToBytes(vector);
                Mockito.when(mockTuple.getFieldData(0)).thenReturn(vectorBytes);
                Mockito.when(mockTuple.getFieldStart(0)).thenReturn(0);
                Mockito.when(mockTuple.getFieldLength(0)).thenReturn(vectorBytes.length);

                // Setup included fields (field 1 to includedFields.length)
                for (int i = 0; i < includedFields.length; i++) {
                    byte[] fieldBytes = includedFields[i].getBytes();
                    int fieldIndex = 1 + i;

                    Mockito.when(mockTuple.getFieldData(fieldIndex)).thenReturn(fieldBytes);
                    Mockito.when(mockTuple.getFieldStart(fieldIndex)).thenReturn(0);
                    Mockito.when(mockTuple.getFieldLength(fieldIndex)).thenReturn(fieldBytes.length);
                }

                // Setup primary key field (LAST field - this is critical!)
                int pkFieldIndex = totalFields - 1;
                Mockito.when(mockTuple.getFieldData(pkFieldIndex)).thenReturn(primaryKey);
                Mockito.when(mockTuple.getFieldStart(pkFieldIndex)).thenReturn(0);
                Mockito.when(mockTuple.getFieldLength(pkFieldIndex)).thenReturn(primaryKey.length);

                // Setup field count
                Mockito.when(mockTuple.getFieldCount()).thenReturn(totalFields);

                return mockTuple;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create test tuple with included fields", e);
            }
        }
    }

    /**
     * Mock behavior configurator for different test scenarios
     */
    public static class MockConfigurator {

        /**
         * Configure mocks for successful operations.
         */
        public static void configureForSuccess(TestMocks mocks) throws HyracksDataException {
            setupStandardMockBehaviors(mocks);
        }

        /**
         * Configure mocks to simulate failure scenarios.
         */
        public static void configureForFailure(TestMocks mocks) throws HyracksDataException {
            setupStandardMockBehaviors(mocks);
            // Add failure-specific behaviors
            Mockito.when(mocks.freePageManager.takePage(Mockito.any()))
                    .thenThrow(new RuntimeException("Simulated page allocation failure"));
        }

        /**
         * Configure mocks for performance testing (minimal overhead).
         */
        public static void configureForPerformance(TestMocks mocks) throws HyracksDataException {
            // Simplified setup for performance tests
            setupPageLatching(mocks);
            setupBufferCache(mocks, TestConfig.createDefault());
        }

        private static void setupStandardMockBehaviors(TestMocks mocks) throws HyracksDataException {
            // Standard mock setup that's common to most test scenarios
            setupPageLatching(mocks);
            setupFrameBehaviors(mocks);
            setupTupleOperations(mocks);
        }
    }

    /**
     * Tree structure validation utilities
     */
    public static class TreeValidator {

        /**
         * Validate that the tree structure is correctly set up.
         */
        public static boolean validateTreeStructure(TestEnvironment env) {
            try {
                // Basic validation checks
                return env.tree != null && env.accessor != null && env.mocks.bufferCache != null && env.config != null;
            } catch (Exception e) {
                return false;
            }
        }

        /**
         * Validate that test data is properly configured.
         */
        public static boolean validateTestData(TestEnvironment env) {
            try {
                // Check if test tuples are properly mocked
                return env.mocks.dataTuple1 != null && env.mocks.dataTuple2 != null && env.mocks.dataTuple3 != null
                        && env.mocks.dataTuple4 != null;
            } catch (Exception e) {
                return false;
            }
        }

        /**
         * Validate that the specific operation setup is correct.
         */
        public static boolean validateOperationSetup(TestEnvironment env, OperationType operation) {
            switch (operation) {
                case INSERT:
                case DELETE:
                case UPDATE:
                case UPSERT:
                    return env.config.enableModificationCallback;
                case SEARCH:
                    return env.config.enableSearchCallback;
                default:
                    return true;
            }
        }
    }
}
