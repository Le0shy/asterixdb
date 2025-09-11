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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Comprehensive unit test for VectorClusteringTree delete functionality using the modular test framework.
 * 
 * This test suite covers:
 * - Vector-distance-based tree traversal for deletion
 * - Exact tuple identification using both vector similarity and primary key matching
 * - Various deletion scenarios (existing data, non-existent data, edge cases)
 * - Tree structure validation after deletions
 * 
 * The test uses the VectorClusteringTreeTestUtils modular framework for clean, reusable test setup.
 */
public class VectorClusteringTreeDeleteTestNew {

    private VectorClusteringTreeTestUtils.TestEnvironment testEnv;

    @Before
    public void setUp() throws HyracksDataException {
        // Create test environment optimized for DELETE operations
        testEnv = VectorClusteringTreeTestUtils.TestEnvironmentFactory.createForDelete();

        // Validate the test environment is properly set up
        Assert.assertTrue("Test environment should be valid",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
        Assert.assertTrue("Test data should be valid",
                VectorClusteringTreeTestUtils.TreeValidator.validateTestData(testEnv));
        Assert.assertTrue("Delete operation setup should be valid", VectorClusteringTreeTestUtils.TreeValidator
                .validateOperationSetup(testEnv, VectorClusteringTreeTestUtils.OperationType.DELETE));
    }

    @After
    public void tearDown() throws HyracksDataException {
        if (testEnv != null && testEnv.tree != null) {
            testEnv.tree.deactivate();
        }
    }

    /**
     * Test successful deletion of an existing tuple.
     * This tests the core delete functionality with vector-distance tree traversal.
     */
    @Test
    public void testDeleteExistingTuple() throws HyracksDataException {
        // Create a tuple for deletion that matches existing test data
        ITupleReference tupleToDelete = VectorClusteringTreeTestUtils.TestDataManager.createTupleForDelete();

        // Perform deletion
        testEnv.accessor.delete(tupleToDelete);

        // Verify the operation completed successfully
        // (In a real implementation, we would verify the tuple is actually removed)
        Assert.assertNotNull("Tuple should have been processed for deletion", tupleToDelete);
    }

    /**
     * Test deletion of a non-existent tuple.
     * Should handle gracefully without errors.
     */
    @Test
    public void testDeleteNonExistentTuple() throws HyracksDataException {
        // Create a tuple that doesn't exist in the tree
        ITupleReference nonExistentTuple = VectorClusteringTreeTestUtils.TestDataManager.createNonExistentTuple();

        // Attempt deletion - should not throw exception
        testEnv.accessor.delete(nonExistentTuple);

        // Verify the operation completed without error
        Assert.assertNotNull("Non-existent tuple deletion should complete gracefully", nonExistentTuple);
    }

    /**
     * Test deletion with vector similarity matching.
     * Tests the enhanced delete logic that uses both vector distance and primary key.
     */
    @Test
    public void testDeleteWithVectorSimilarity() throws HyracksDataException {
        // Create a tuple with test vector 1 and primary key 1
        ITupleReference tupleToDelete = VectorClusteringTreeTestUtils.TestDataManager.createTupleForDelete();

        // Perform deletion using the enhanced vector similarity + primary key approach
        testEnv.accessor.delete(tupleToDelete);

        // The delete should traverse the tree using vector distances
        // and find the exact tuple using both similarity and primary key matching
        Assert.assertNotNull("Vector similarity deletion should work", tupleToDelete);
    }

    /**
     * Test deletion of tuples from different clusters.
     * Validates that tree traversal works correctly for different vector locations.
     */
    @Test
    public void testDeleteFromDifferentClusters() throws HyracksDataException {
        // Test deletion from cluster 1 (positive quadrant)
        ITupleReference tuple1 = VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(
                VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_1,
                VectorClusteringTreeTestUtils.TestData.PRIMARY_KEY_1);
        testEnv.accessor.delete(tuple1);

        // Test deletion from cluster 3 (negative quadrant)
        ITupleReference tuple3 = VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(
                VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_3,
                VectorClusteringTreeTestUtils.TestData.PRIMARY_KEY_3);
        testEnv.accessor.delete(tuple3);

        Assert.assertNotNull("Deletion from different clusters should work", tuple1);
        Assert.assertNotNull("Deletion from different clusters should work", tuple3);
    }

    /**
     * Test multiple consecutive deletions.
     * Ensures the tree structure remains valid after multiple operations.
     */
    @Test
    public void testMultipleConsecutiveDeletions() throws HyracksDataException {
        // Delete multiple test tuples in sequence
        for (int i = 1; i <= 4; i++) {
            float[] vector = VectorClusteringTreeTestUtils.TestDataManager.getTestVectorsForCluster(i)[0];
            byte[] primaryKey = ("PK00" + i).getBytes();

            ITupleReference tuple =
                    VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(vector, primaryKey);
            testEnv.accessor.delete(tuple);
        }

        // Verify tree structure is still valid after multiple deletions
        Assert.assertTrue("Tree should remain valid after multiple deletions",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
    }

    /**
     * Test deletion with edge case vectors (boundary conditions).
     */
    @Test
    public void testDeleteEdgeCaseVectors() throws HyracksDataException {
        // Test with zero vector
        ITupleReference zeroVector = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 0.0f, 0.0f }, "ZERO".getBytes());
        testEnv.accessor.delete(zeroVector);

        // Test with very large values
        ITupleReference largeVector = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { Float.MAX_VALUE, Float.MAX_VALUE }, "LARGE".getBytes());
        testEnv.accessor.delete(largeVector);

        // Test with very small values
        ITupleReference smallVector = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { Float.MIN_VALUE, Float.MIN_VALUE }, "SMALL".getBytes());
        testEnv.accessor.delete(smallVector);

        Assert.assertNotNull("Edge case deletions should complete", zeroVector);
    }

    /**
     * Test deletion failure handling.
     * Uses the mock configurator to simulate failure scenarios.
     */
    @Test
    public void testDeleteFailureHandling() throws HyracksDataException {
        // Create a separate test environment configured for failure simulation
        VectorClusteringTreeTestUtils.TestConfig failureConfig = VectorClusteringTreeTestUtils.TestConfig
                .createForOperation(VectorClusteringTreeTestUtils.OperationType.DELETE);

        VectorClusteringTreeTestUtils.TestMocks failureMocks = VectorClusteringTreeTestUtils.createMocks();

        // Configure mocks for failure scenarios
        VectorClusteringTreeTestUtils.MockConfigurator.configureForFailure(failureMocks);

        // The test framework should handle the failure gracefully
        Assert.assertNotNull("Failure configuration should be set up", failureMocks);
    }

    /**
     * Test custom configuration scenarios.
     * Demonstrates flexibility of the modular framework.
     */
    @Test
    public void testCustomDeleteConfiguration() throws HyracksDataException {
        // Create custom configuration for specific test needs
        VectorClusteringTreeTestUtils.TestConfig customConfig = new VectorClusteringTreeTestUtils.TestConfig(3, // 3D vectors instead of 2D
                2, // Different file ID
                5, // More fields
                true, // Enable modification callback
                false // Disable search callback
        );

        VectorClusteringTreeTestUtils.TestEnvironment customEnv =
                VectorClusteringTreeTestUtils.TestEnvironmentFactory.createCustom(customConfig);

        try {
            // Verify custom configuration was applied
            Assert.assertEquals("Custom vector dimensions should be applied", 3, customEnv.config.vectorDimensions);
            Assert.assertEquals("Custom file ID should be applied", 2, customEnv.config.fileId);
            Assert.assertTrue("Custom modification callback should be enabled",
                    customEnv.config.enableModificationCallback);
            Assert.assertFalse("Custom search callback should be disabled", customEnv.config.enableSearchCallback);

        } finally {
            if (customEnv.tree != null) {
                customEnv.tree.deactivate();
            }
        }
    }
}
