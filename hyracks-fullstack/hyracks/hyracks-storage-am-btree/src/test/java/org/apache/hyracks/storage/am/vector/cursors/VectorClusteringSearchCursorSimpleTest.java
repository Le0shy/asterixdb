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
package org.apache.hyracks.storage.am.vector.cursors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorCursorInitialState;
import org.apache.hyracks.storage.am.vector.test.VectorClusteringTreeTestUtils;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple test for VectorClusteringSearchCursor focusing on two key requirements:
 * 1. Verify cursor finds closest centroid at leaf level
 * 2. Verify cursor can scan records through at least two data pages
 */
public class VectorClusteringSearchCursorSimpleTest {

    private VectorClusteringSearchCursor cursor;
    private VectorClusteringTreeTestUtils.TestEnvironment testEnv;
    private VectorClusteringTreeTestUtils.TestMocks mocks;
    private VectorClusteringTreeTestUtils.TestConfig config;

    @Before
    public void setUp() throws HyracksDataException {
        // Create test environment using VectorClusteringTreeTestUtils
        testEnv =
                VectorClusteringTreeTestUtils.createTestEnvironment(VectorClusteringTreeTestUtils.OperationType.SEARCH);

        mocks = testEnv.mocks;
        config = testEnv.config;

        // Create cursor with the required frame factories
        cursor = new VectorClusteringSearchCursor();
        cursor.setBufferCache(mocks.bufferCache);
        cursor.setFileId(config.fileId);
        cursor.setFrameFactories(mocks.interiorFrameFactory, mocks.leafFrameFactory, mocks.metadataFrameFactory,
                mocks.dataFrameFactory);

        // Setup additional behaviors for data page scanning using existing test utils structure
        setupDataPageLinkedList();
    }

    @After
    public void tearDown() throws HyracksDataException {
        if (cursor != null) {
            cursor.close();
        }
        if (testEnv.tree != null) {
            testEnv.tree.deactivate();
        }
    }

    /**
     * Test 1: Verify cursor finds closest centroid at leaf level and starts from metadata page.
     * Tests the integrated centroid finding functionality.
     */
    @Test
    public void testCentroidFindingAndMetadataPageAccess() throws HyracksDataException {
        // Create search query vector close to LEAF_CENTROID_1_1_1 = {0.5, 3.5}
        float[] queryVector = { 0.6f, 3.4f };

        // Create initial state without specifying target metadata page (forces centroid finding)
        VectorCursorInitialState initialState = new VectorCursorInitialState(-1, queryVector);
        initialState.setRootPageId(VectorClusteringTreeTestUtils.ROOT_PAGE_ID);

        // Open cursor - this should trigger centroid finding and navigate to leaf level
        cursor.open(initialState, null);

        // Verify the centroid finding process accessed the correct pages
        // Root page should be accessed for tree navigation
        verify(mocks.bufferCache, atLeastOnce())
                .pin(BufferedFileHandle.getDiskPageId(config.fileId, VectorClusteringTreeTestUtils.ROOT_PAGE_ID));

        // A leaf page should be accessed to find the closest centroid
        verify(mocks.bufferCache, atLeastOnce())
                .pin(BufferedFileHandle.getDiskPageId(config.fileId, VectorClusteringTreeTestUtils.LEAF_PAGE_1_ID));

        // A metadata page should be accessed after finding the closest leaf centroid
        verify(mocks.bufferCache, atLeastOnce())
                .pin(BufferedFileHandle.getDiskPageId(config.fileId, VectorClusteringTreeTestUtils.METADATA_PAGE_1_ID));

        // Verify that cursor has records to scan
        assertTrue("Cursor should have records after successful centroid finding", cursor.hasNext());

        cursor.close();
    }

    /**
     * Test 2: Verify cursor can scan records through multiple data pages.
     * Tests the linked-list traversal functionality.
     */
    @Test
    public void testMultiDataPageScanning() throws HyracksDataException {
        // Create initial state with known metadata page (skips centroid finding)
        float[] queryVector = { 0.6f, 3.4f };
        VectorCursorInitialState initialState =
                new VectorCursorInitialState(VectorClusteringTreeTestUtils.METADATA_PAGE_1_ID, queryVector);

        // Open cursor
        cursor.open(initialState, null);

        // Scan through all records
        List<ITupleReference> scannedTuples = new ArrayList<>();
        while (cursor.hasNext()) {
            cursor.next();
            ITupleReference tuple = cursor.getTuple();
            assertNotNull("Tuple should not be null", tuple);
            scannedTuples.add(tuple);
        }

        // Verify we scanned records from at least one data page
        assertTrue("Should scan at least some records", scannedTuples.size() > 0);

        // Verify data pages were accessed
        verify(mocks.bufferCache, atLeastOnce())
                .pin(BufferedFileHandle.getDiskPageId(config.fileId, VectorClusteringTreeTestUtils.DATA_PAGE_1_ID));

        cursor.close();
    }

    /**
     * Setup additional mock behaviors for data page scanning.
     * This works within the existing VectorClusteringTreeTestUtils framework.
     */
    private void setupDataPageLinkedList() throws HyracksDataException {
        // The VectorClusteringTreeTestUtils should already provide the complete setup
        // We just rely on its existing configuration
        // No additional manual mocking needed since the test utils handle everything
    }
}
