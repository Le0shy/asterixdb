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
package org.apache.hyracks.storage.am.vector.impls;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Static structure initializer for VectorClusteringTree for unit testing purposes.
 * This class creates predictable multi-level tree structures (root/interior/leaf/metadata/data pages)
 * without modifying the core VectorClusteringTree implementation.
 * 
 * Pattern based on BTreeNSMBulkLoader for creating static test structures.
 */
public class VectorClusteringTreeStaticInitializer {

    private final VectorClusteringTree vectorTree;
    private final IBufferCache bufferCache;
    private final IPageManager pageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    @SuppressWarnings("unused")
    private final MultiComparator cmp;
    private final int fileId;
    private final List<TestPage> testPages;

    // Tuple writers for different frame types
    @SuppressWarnings("unused")
    private final org.apache.hyracks.storage.am.vector.tuples.VectorClusteringLeafTupleWriter leafTupleWriter;
    @SuppressWarnings("unused")
    private final org.apache.hyracks.storage.am.vector.tuples.VectorClusteringInteriorTupleWriter interiorTupleWriter;
    @SuppressWarnings("unused")
    private final org.apache.hyracks.storage.am.vector.tuples.VectorClusteringMetadataTupleWriter metadataTupleWriter;

    /**
     * Represents a test page in the vector tree structure
     */
    public static class TestPage {
        public final int pageId;
        public final ICachedPage page;
        public final PageType type;
        public final List<ITupleReference> tuples;

        public enum PageType {
            ROOT,
            INTERIOR,
            LEAF,
            METADATA,
            VECTOR_DATA
        }

        public TestPage(int pageId, ICachedPage page, PageType type) {
            this.pageId = pageId;
            this.page = page;
            this.type = type;
            this.tuples = new ArrayList<>();
        }

        public void addTuple(ITupleReference tuple) {
            tuples.add(tuple);
        }
    }

    /**
     * Configuration for the static tree structure
     */
    public static class TreeStructureConfig {
        public final int numLeafPages;
        public final int numInteriorPages;
        public final int numVectorDataPages;
        public final int tuplesPerLeaf;
        public final boolean createMultiLevel;

        public TreeStructureConfig(int numLeafPages, int numInteriorPages, int numVectorDataPages, int tuplesPerLeaf,
                boolean createMultiLevel) {
            this.numLeafPages = numLeafPages;
            this.numInteriorPages = numInteriorPages;
            this.numVectorDataPages = numVectorDataPages;
            this.tuplesPerLeaf = tuplesPerLeaf;
            this.createMultiLevel = createMultiLevel;
        }

        // Predefined configurations for common test scenarios
        public static TreeStructureConfig singleLeaf() {
            return new TreeStructureConfig(1, 0, 1, 5, false);
        }

        public static TreeStructureConfig multipleLeaves() {
            return new TreeStructureConfig(3, 1, 3, 5, true);
        }

        public static TreeStructureConfig deepTree() {
            return new TreeStructureConfig(5, 3, 5, 10, true);
        }
    }

    public VectorClusteringTreeStaticInitializer(VectorClusteringTree vectorTree) throws HyracksDataException {
        this.vectorTree = vectorTree;
        this.bufferCache = vectorTree.getBufferCache();
        this.pageManager = vectorTree.getPageManager();
        this.metaFrame = pageManager.createMetadataFrame();
        this.cmp = MultiComparator.create(vectorTree.getCmpFactories());
        this.fileId = vectorTree.getFileId();
        this.testPages = new ArrayList<>();

        // Initialize tuple writers for different frame types
        this.leafTupleWriter = (org.apache.hyracks.storage.am.vector.tuples.VectorClusteringLeafTupleWriter) vectorTree
                .getLeafFrameFactory().createFrame().getTupleWriter();
        this.interiorTupleWriter =
                (org.apache.hyracks.storage.am.vector.tuples.VectorClusteringInteriorTupleWriter) vectorTree
                        .getInteriorFrameFactory().createFrame().getTupleWriter();
        this.metadataTupleWriter =
                (org.apache.hyracks.storage.am.vector.tuples.VectorClusteringMetadataTupleWriter) vectorTree
                        .getMetadataFrameFactory().createFrame().getTupleWriter();
    }

    /**
     * Initialize a static tree structure with the given configuration and tuples
     */
    public void initializeStaticStructure(TreeStructureConfig config, List<ITupleReference> tuples)
            throws HyracksDataException {

        if (tuples.isEmpty()) {
            throw new IllegalArgumentException("Cannot initialize tree with empty tuple list");
        }

        // Clear any existing structure
        cleanup();

        // Create pages based on configuration
        if (config.createMultiLevel) {
            initializeMultiLevelTree(config, tuples);
        } else {
            initializeSingleLevelTree(config, tuples);
        }

        // Set the root page in the tree using a setter method
        if (!testPages.isEmpty()) {
            TestPage rootPage = findPageByType(TestPage.PageType.ROOT);
            if (rootPage == null) {
                // If no explicit root, use the first interior or leaf page as root
                rootPage = testPages.stream()
                        .filter(p -> p.type == TestPage.PageType.INTERIOR || p.type == TestPage.PageType.LEAF)
                        .findFirst().orElse(testPages.get(0));
            }
            setRootPageId(rootPage.pageId);
        }
    }

    /**
     * Initialize a single-level tree (root is a leaf page)
     */
    private void initializeSingleLevelTree(TreeStructureConfig config, List<ITupleReference> tuples)
            throws HyracksDataException {

        // Create leaf pages
        for (int i = 0; i < config.numLeafPages; i++) {
            TestPage leafPage = createPage(TestPage.PageType.LEAF);

            // Add tuples to this leaf page
            int startIndex = i * config.tuplesPerLeaf;
            int endIndex = Math.min(startIndex + config.tuplesPerLeaf, tuples.size());
            for (int j = startIndex; j < endIndex; j++) {
                leafPage.addTuple(tuples.get(j));
            }

            // Initialize the page with tuples
            initializeLeafPage(leafPage);
        }

        // Create associated vector data pages
        for (int i = 0; i < config.numVectorDataPages; i++) {
            TestPage dataPage = createPage(TestPage.PageType.VECTOR_DATA);
            initializeVectorDataPage(dataPage);
        }
    }

    /**
     * Initialize a multi-level tree with interior and leaf pages
     */
    private void initializeMultiLevelTree(TreeStructureConfig config, List<ITupleReference> tuples)
            throws HyracksDataException {

        List<TestPage> leafPages = new ArrayList<>();

        // Create leaf pages first
        for (int i = 0; i < config.numLeafPages; i++) {
            TestPage leafPage = createPage(TestPage.PageType.LEAF);
            leafPages.add(leafPage);

            // Add tuples to this leaf page
            int startIndex = i * config.tuplesPerLeaf;
            int endIndex = Math.min(startIndex + config.tuplesPerLeaf, tuples.size());
            for (int j = startIndex; j < endIndex; j++) {
                leafPage.addTuple(tuples.get(j));
            }

            initializeLeafPage(leafPage);
        }

        // Create interior pages
        List<TestPage> currentLevel = leafPages;
        while (currentLevel.size() > 1 || testPages.stream().noneMatch(p -> p.type == TestPage.PageType.ROOT)) {
            List<TestPage> nextLevel = new ArrayList<>();

            // Group current level pages under interior/root pages
            for (int i = 0; i < currentLevel.size(); i += 2) {
                TestPage.PageType pageType = (currentLevel.size() <= 2 && nextLevel.isEmpty()) ? TestPage.PageType.ROOT
                        : TestPage.PageType.INTERIOR;

                TestPage parentPage = createPage(pageType);

                // Add references to child pages
                parentPage.addTuple(currentLevel.get(i).tuples.get(0)); // First tuple of left child
                if (i + 1 < currentLevel.size()) {
                    parentPage.addTuple(currentLevel.get(i + 1).tuples.get(0)); // First tuple of right child
                }

                initializeInteriorPage(parentPage, currentLevel.get(i).pageId,
                        i + 1 < currentLevel.size() ? currentLevel.get(i + 1).pageId : -1);

                nextLevel.add(parentPage);
            }

            currentLevel = nextLevel;
        }

        // Create vector data pages
        for (int i = 0; i < config.numVectorDataPages; i++) {
            TestPage dataPage = createPage(TestPage.PageType.VECTOR_DATA);
            initializeVectorDataPage(dataPage);
        }
    }

    /**
     * Create a new page of the specified type
     */
    private TestPage createPage(TestPage.PageType type) throws HyracksDataException {
        int pageId = pageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
        ICachedPage page = bufferCache.confiscatePage(dpid);

        TestPage testPage = new TestPage(pageId, page, type);
        testPages.add(testPage);

        return testPage;
    }

    /**
     * Initialize a leaf page with its tuples
     */
    private void initializeLeafPage(TestPage leafPage) throws HyracksDataException {
        // Use VectorClusteringLeafFrame for proper initialization
        org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrame leafFrame =
                (org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrame) vectorTree.getLeafFrameFactory()
                        .createFrame();

        leafFrame.setPage(leafPage.page);
        leafFrame.initBuffer((byte) 0); // Leaf level is 0

        // Set a test centroid for this leaf page
        double[] testCentroid = generateTestCentroid(leafPage.pageId);
        leafFrame.setCentroid(testCentroid);
        leafFrame.setClusterId(leafPage.pageId); // Use page ID as cluster ID for testing

        // Insert tuples into the leaf frame using proper frame methods
        for (ITupleReference tuple : leafPage.tuples) {
            if (leafFrame.hasSpaceInsert(
                    tuple) != org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus.INSUFFICIENT_SPACE) {
                int insertIndex = leafFrame.findInsertTupleIndex(tuple);
                leafFrame.insert(tuple, insertIndex);
            }
        }
    }

    /**
     * Initialize an interior page with child page references
     */
    private void initializeInteriorPage(TestPage interiorPage, int leftChildId, int rightChildId)
            throws HyracksDataException {
        // Use VectorClusteringInteriorFrame for proper initialization
        org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrame interiorFrame =
                (org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrame) vectorTree
                        .getInteriorFrameFactory().createFrame();

        interiorFrame.setPage(interiorPage.page);
        interiorFrame.initBuffer((byte) 1); // Interior level is 1 or higher

        // Set a test centroid for this interior page
        double[] testCentroid = generateTestCentroid(interiorPage.pageId);
        interiorFrame.setCentroid(testCentroid);
        interiorFrame.setClusterId(interiorPage.pageId); // Use page ID as cluster ID for testing

        // Insert cluster entries with child page pointers
        for (int i = 0; i < interiorPage.tuples.size(); i++) {
            ITupleReference tuple = interiorPage.tuples.get(i);
            if (interiorFrame.hasSpaceInsert(
                    tuple) != org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus.INSUFFICIENT_SPACE) {
                int insertIndex = interiorFrame.findInsertTupleIndex(tuple);
                interiorFrame.insert(tuple, insertIndex);

                // Set child page pointer - use left child for first tuple, right for second
                int childPageId = (i == 0) ? leftChildId : rightChildId;
                if (childPageId > 0) {
                    interiorFrame.setChildPageId(insertIndex, childPageId);
                }
            }
        }
    }

    /**
     * Initialize a vector data page
     */
    private void initializeVectorDataPage(TestPage dataPage) throws HyracksDataException {
        // Use VectorClusteringDataFrame for vector data pages
        org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrame dataFrame =
                (org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrame) vectorTree.getDataFrameFactory()
                        .createFrame();

        dataFrame.setPage(dataPage.page);
        dataFrame.initBuffer((byte) 0); // Data pages are at level 0

        // Set test centroid and cluster ID
        double[] testCentroid = generateTestCentroid(dataPage.pageId);
        dataFrame.setCentroid(testCentroid);
        dataFrame.setClusterId(dataPage.pageId);
    }

    /**
     * Find a page by type
     */
    private TestPage findPageByType(TestPage.PageType type) {
        return testPages.stream().filter(page -> page.type == type).findFirst().orElse(null);
    }

    /**
     * Get all pages of a specific type
     */
    public List<TestPage> getPagesByType(TestPage.PageType type) {
        return testPages.stream().filter(page -> page.type == type).collect(ArrayList::new,
                (list, page) -> list.add(page), ArrayList::addAll);
    }

    /**
     * Get all test pages
     */
    public List<TestPage> getAllPages() {
        return new ArrayList<>(testPages);
    }

    /**
     * Generate a test centroid based on page ID for predictable testing
     */
    private double[] generateTestCentroid(int pageId) {
        double[] centroid = new double[4]; // Default 4D centroids for testing
        for (int i = 0; i < centroid.length; i++) {
            centroid[i] = pageId * 10.0 + i; // Generate predictable values
        }
        return centroid;
    }

    /**
     * Create a test tuple for leaf frames (format: <cid, centroid, metadata_ptr>)
     */
    @SuppressWarnings("unused")
    private ITupleReference createLeafTestTuple(int clusterId, double[] centroid, int metadataPageId) {
        // Create a simple tuple reference for testing
        // In practice, you would use proper tuple builders and serializers
        return new org.apache.hyracks.storage.am.common.tuples.SimpleTupleReference();
    }

    /**
     * Create a test tuple for interior frames (format: <cid, centroid, child_ptr>)
     */
    @SuppressWarnings("unused")
    private ITupleReference createInteriorTestTuple(int clusterId, double[] centroid, int childPageId) {
        // Create a simple tuple reference for testing
        // In practice, you would use proper tuple builders and serializers
        return new org.apache.hyracks.storage.am.common.tuples.SimpleTupleReference();
    }

    /**
     * Create a test tuple for metadata frames (format: <max_distance, data_page_ptr>)
     */
    @SuppressWarnings("unused")
    private ITupleReference createMetadataTestTuple(float maxDistance, int dataPageId) {
        // Create a simple tuple reference for testing
        return new org.apache.hyracks.storage.am.common.tuples.SimpleTupleReference();
    }

    /**
     * Set the root page ID using reflection to access the protected field
     */
    private void setRootPageId(int rootPageId) throws HyracksDataException {
        try {
            // Use reflection to access the protected rootPage field
            java.lang.reflect.Field rootPageField = AbstractTreeIndex.class.getDeclaredField("rootPage");
            rootPageField.setAccessible(true);
            rootPageField.setInt(vectorTree, rootPageId);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.CANNOT_ACTIVATE_ACTIVE_INDEX,
                    e);
        }
    }

    /**
     * Get the root page ID
     */
    public int getRootPageId() {
        TestPage rootPage = findPageByType(TestPage.PageType.ROOT);
        if (rootPage != null) {
            return rootPage.pageId;
        }

        // If no explicit root, return the first interior or leaf page
        return testPages.stream().filter(p -> p.type == TestPage.PageType.INTERIOR || p.type == TestPage.PageType.LEAF)
                .findFirst().map(p -> p.pageId).orElse(-1);
    }

    /**
     * Clean up all test pages
     */
    public void cleanup() throws HyracksDataException {
        for (TestPage testPage : testPages) {
            if (testPage.page != null) {
                bufferCache.returnPage(testPage.page, false);
            }
        }
        testPages.clear();
    }

    /**
     * Force write all pages to disk
     */
    public void flush() throws HyracksDataException {
        // In the test environment, we don't need to manually flush pages
        // The buffer cache handles this automatically
        // This method is provided for completeness
    }
}
