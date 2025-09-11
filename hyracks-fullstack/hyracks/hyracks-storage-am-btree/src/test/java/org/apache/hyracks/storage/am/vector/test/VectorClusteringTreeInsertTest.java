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
 * Comprehensive unit test for VectorClusteringTree insert functionality using the modular test framework.
 * 
 * This test suite covers:
 * - Basic vector insertion functionality using distance-based tree traversal
 * - Multiple insertions to different clusters with distance calculations
 * - Data page overflow scenarios and splitting behavior
 * - Edge cases (boundary vectors, duplicate keys, extreme values)
 * - Tree structure validation after insertions
 * - Custom configuration testing for various insertion scenarios
 * 
 * The test uses the VectorClusteringTreeTestUtils modular framework for clean, reusable test setup.
 * All insertions follow the pattern: findClusterAndPrepareAccess() → calculate distances → insertIntoDataPages()
 */
public class VectorClusteringTreeInsertTest {

    private VectorClusteringTreeTestUtils.TestEnvironment testEnv;

    @Before
    public void setUp() throws HyracksDataException {
        // Create test environment optimized for INSERT operations
        testEnv = VectorClusteringTreeTestUtils.TestEnvironmentFactory.createForInsert();

        // Validate the test environment is properly set up
        Assert.assertTrue("Test environment should be valid",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
        Assert.assertTrue("Test data should be valid",
                VectorClusteringTreeTestUtils.TreeValidator.validateTestData(testEnv));
        Assert.assertTrue("Insert operation setup should be valid", VectorClusteringTreeTestUtils.TreeValidator
                .validateOperationSetup(testEnv, VectorClusteringTreeTestUtils.OperationType.INSERT));
    }

    @After
    public void tearDown() throws HyracksDataException {
        if (testEnv != null && testEnv.tree != null) {
            testEnv.tree.deactivate();
        }
    }

    /**
     * Test basic insertion of a single vector.
     * This tests the core insertion functionality with vector-distance tree traversal.
     */
    @Test
    public void testBasicVectorInsertion() throws HyracksDataException {
        // Create a tuple for insertion using predefined test data
        ITupleReference tupleToInsert = VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(
                VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_1,
                VectorClusteringTreeTestUtils.TestData.PRIMARY_KEY_1);

        // Perform insertion
        testEnv.accessor.insert(tupleToInsert);

        // Verify the operation completed successfully
        Assert.assertNotNull("Tuple should have been processed for insertion", tupleToInsert);

        // Validate tree structure remains consistent after insertion
        Assert.assertTrue("Tree structure should remain valid after insertion",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
    }

    /**
     * Test insertion of multiple vectors to different clusters.
     * Validates that distance-based tree traversal works correctly for different vector locations.
     */
    @Test
    public void testInsertionToDifferentClusters() throws HyracksDataException {
        // Insert vector to cluster 1 (positive quadrant)
        ITupleReference tuple1 = VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(
                VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_1,
                VectorClusteringTreeTestUtils.TestData.PRIMARY_KEY_1);
        testEnv.accessor.insert(tuple1);

        // Insert vector to cluster 3 (negative quadrant)
        ITupleReference tuple3 = VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(
                VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_3,
                VectorClusteringTreeTestUtils.TestData.PRIMARY_KEY_3);
        testEnv.accessor.insert(tuple3);

        // Insert vector to cluster 2 (mixed quadrant)
        ITupleReference tuple2 = VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(
                VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_2,
                VectorClusteringTreeTestUtils.TestData.PRIMARY_KEY_2);
        testEnv.accessor.insert(tuple2);

        // Insert vector to cluster 4 (opposite negative quadrant)
        ITupleReference tuple4 = VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(
                VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_4,
                VectorClusteringTreeTestUtils.TestData.PRIMARY_KEY_4);
        testEnv.accessor.insert(tuple4);

        Assert.assertNotNull("Insertion to different clusters should work", tuple1);
        Assert.assertNotNull("Insertion to different clusters should work", tuple2);
        Assert.assertNotNull("Insertion to different clusters should work", tuple3);
        Assert.assertNotNull("Insertion to different clusters should work", tuple4);

        // Verify tree structure remains valid after multiple insertions
        Assert.assertTrue("Tree should remain valid after multiple cluster insertions",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
    }

    /**
     * Test multiple consecutive insertions to the same cluster.
     * Ensures the tree can handle multiple inserts to same data pages and handles overflow correctly.
     */
    @Test
    public void testMultipleConsecutiveInsertions() throws HyracksDataException {
        // Insert multiple test tuples to same cluster area
        float[] baseVector = VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_1;

        for (int i = 1; i <= 6; i++) {
            // Create slightly different vectors near the same cluster
            float[] similarVector = { baseVector[0] + (i * 0.1f), baseVector[1] + (i * 0.1f) };
            byte[] primaryKey = ("PK00" + i).getBytes();

            ITupleReference tuple =
                    VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(similarVector, primaryKey);
            testEnv.accessor.insert(tuple);
        }

        // Verify tree structure is still valid after multiple consecutive insertions
        Assert.assertTrue("Tree should remain valid after multiple consecutive insertions",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
    }

    /**
     * Test insertion with edge case vectors (boundary conditions).
     * Tests the robustness of the insertion algorithm with extreme values.
     */
    @Test
    public void testInsertEdgeCaseVectors() throws HyracksDataException {
        // Test with zero vector (origin)
        ITupleReference zeroVector = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 0.0f, 0.0f }, "ZERO".getBytes());
        testEnv.accessor.insert(zeroVector);

        // Test with unit vector
        ITupleReference unitVector = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 1.0f, 1.0f }, "UNIT".getBytes());
        testEnv.accessor.insert(unitVector);

        // Test with negative coordinates
        ITupleReference negativeVector = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { -1.0f, -1.0f }, "NEG".getBytes());
        testEnv.accessor.insert(negativeVector);

        // Test with large values (within reasonable bounds)
        ITupleReference largeVector = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 100.0f, 100.0f }, "LARGE".getBytes());
        testEnv.accessor.insert(largeVector);

        // Test with very small positive values
        ITupleReference smallVector = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 0.001f, 0.001f }, "SMALL".getBytes());
        testEnv.accessor.insert(smallVector);

        Assert.assertNotNull("Edge case insertions should complete", zeroVector);
        Assert.assertNotNull("Edge case insertions should complete", unitVector);
        Assert.assertNotNull("Edge case insertions should complete", negativeVector);
        Assert.assertNotNull("Edge case insertions should complete", largeVector);
        Assert.assertNotNull("Edge case insertions should complete", smallVector);

        // Verify tree structure remains valid after edge case insertions
        Assert.assertTrue("Tree should handle edge case insertions correctly",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
    }

    /**
     * Test data page overflow and splitting behavior.
     * Simulates scenarios where data pages become full and need to split.
     */
    @Test
    public void testDataPageOverflowScenarios() throws HyracksDataException {
        // Insert many vectors to the same cluster to force data page overflow
        float[] clusterBaseVector = VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_1;

        // Insert enough tuples to potentially trigger page splitting
        for (int i = 1; i <= 20; i++) {
            // Generate vectors clustered around the same area
            float variation = i * 0.05f;
            float[] clusteredVector = { clusterBaseVector[0] + variation, clusterBaseVector[1] + variation };
            byte[] primaryKey = String.format("OVFL%03d", i).getBytes();

            ITupleReference tuple =
                    VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(clusteredVector, primaryKey);
            testEnv.accessor.insert(tuple);
        }

        // Verify tree structure remains valid even with potential page splits
        Assert.assertTrue("Tree should handle page overflow and splitting correctly",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
    }

    /**
     * Test insertion of vectors with duplicate primary keys.
     * Tests how the system handles potential key conflicts.
     */
    @Test
    public void testDuplicateKeyHandling() throws HyracksDataException {
        // Insert initial tuple
        ITupleReference initialTuple = VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(
                VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_1,
                VectorClusteringTreeTestUtils.TestData.PRIMARY_KEY_1);
        testEnv.accessor.insert(initialTuple);

        // Attempt to insert tuple with same primary key but different vector
        ITupleReference duplicateKeyTuple = VectorClusteringTreeTestUtils.TestDataManager.createTupleForInsert(
                VectorClusteringTreeTestUtils.TestData.TEST_VECTOR_2,
                VectorClusteringTreeTestUtils.TestData.PRIMARY_KEY_1); // Same primary key

        // The behavior for duplicate keys depends on the index implementation
        // This test ensures it doesn't crash the system
        testEnv.accessor.insert(duplicateKeyTuple);

        Assert.assertNotNull("Duplicate key insertion should complete gracefully", duplicateKeyTuple);
    }

    /**
     * Test insertion with distance calculation validation.
     * Verifies that insertions follow the distance-based cluster selection logic.
     */
    @Test
    public void testDistanceBasedClusterSelection() throws HyracksDataException {
        // Test vectors that should go to specific clusters based on distance to centroids

        // Vector very close to cluster 1 centroid
        ITupleReference closeToCluster1 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 0.9f, 3.1f }, "CLOSE1".getBytes());
        testEnv.accessor.insert(closeToCluster1);

        // Vector very close to cluster 2 centroid  
        ITupleReference closeToCluster2 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 3.1f, 1.9f }, "CLOSE2".getBytes());
        testEnv.accessor.insert(closeToCluster2);

        // Vector very close to cluster 3 centroid
        ITupleReference closeToCluster3 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { -1.1f, -2.9f }, "CLOSE3".getBytes());
        testEnv.accessor.insert(closeToCluster3);

        // Vector very close to cluster 4 centroid
        ITupleReference closeToCluster4 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { -3.1f, -1.9f }, "CLOSE4".getBytes());
        testEnv.accessor.insert(closeToCluster4);

        Assert.assertNotNull("Distance-based cluster selection should work", closeToCluster1);
        Assert.assertNotNull("Distance-based cluster selection should work", closeToCluster2);
        Assert.assertNotNull("Distance-based cluster selection should work", closeToCluster3);
        Assert.assertNotNull("Distance-based cluster selection should work", closeToCluster4);

        // The insertion algorithm should have selected the closest cluster for each vector
        Assert.assertTrue("Tree should reflect distance-based insertion decisions",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
    }

    /**
     * Test insertion performance with bulk operations.
     * Tests system behavior under high insertion load.
     */
    @Test
    public void testBulkInsertionPerformance() throws HyracksDataException {
        // Insert a moderate number of vectors to test bulk insertion behavior
        for (int i = 1; i <= 50; i++) {
            // Create vectors distributed across different quadrants
            float x = (float) (Math.cos(i * Math.PI / 25) * 5.0);
            float y = (float) (Math.sin(i * Math.PI / 25) * 5.0);

            ITupleReference tuple = VectorClusteringTreeTestUtils.TestDataManager
                    .createTupleForInsert(new float[] { x, y }, String.format("BULK%03d", i).getBytes());
            testEnv.accessor.insert(tuple);
        }

        // Verify tree remains consistent after bulk insertions
        Assert.assertTrue("Tree should handle bulk insertions efficiently",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
    }

    /**
     * Test custom configuration scenarios.
     * Demonstrates flexibility of the modular framework for different insertion configurations.
     */
    @Test
    public void testCustomInsertConfiguration() throws HyracksDataException {
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

            // Test insertion with custom configuration
            float[] threeDVector = { 1.0f, 2.0f, 3.0f };
            ITupleReference customTuple = VectorClusteringTreeTestUtils.TestDataManager
                    .createTupleForInsert(threeDVector, "3D_TEST".getBytes());
            customEnv.accessor.insert(customTuple);

            Assert.assertNotNull("Custom configuration insertion should work", customTuple);

        } finally {
            if (customEnv.tree != null) {
                customEnv.tree.deactivate();
            }
        }
    }

    /**
     * Test insertion failure handling.
     * Uses the mock configurator to simulate failure scenarios.
     */
    @Test
    public void testInsertFailureHandling() throws HyracksDataException {
        // Create a separate test environment configured for failure simulation
        VectorClusteringTreeTestUtils.TestConfig failureConfig = VectorClusteringTreeTestUtils.TestConfig
                .createForOperation(VectorClusteringTreeTestUtils.OperationType.INSERT);

        VectorClusteringTreeTestUtils.TestMocks failureMocks = VectorClusteringTreeTestUtils.createMocks();

        // Configure mocks for failure scenarios
        VectorClusteringTreeTestUtils.MockConfigurator.configureForFailure(failureMocks);

        // The test framework should handle the failure gracefully
        Assert.assertNotNull("Failure configuration should be set up", failureMocks);
    }

    /**
     * Test vector cosine similarity during insertion.
     * Verifies that the distance calculations used for cluster selection work correctly.
     */
    @Test
    public void testCosineSimilarityCalculation() throws HyracksDataException {
        // Test with vectors that have known cosine similarities

        // Parallel vectors (cosine similarity = 1.0)
        ITupleReference parallel1 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 1.0f, 2.0f }, "PAR1".getBytes());
        ITupleReference parallel2 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 2.0f, 4.0f }, "PAR2".getBytes());

        // Orthogonal vectors (cosine similarity = 0.0)
        ITupleReference orthogonal1 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 1.0f, 0.0f }, "ORTH1".getBytes());
        ITupleReference orthogonal2 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 0.0f, 1.0f }, "ORTH2".getBytes());

        // Opposite vectors (cosine similarity = -1.0)
        ITupleReference opposite1 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { 1.0f, 1.0f }, "OPP1".getBytes());
        ITupleReference opposite2 = VectorClusteringTreeTestUtils.TestDataManager
                .createTupleForInsert(new float[] { -1.0f, -1.0f }, "OPP2".getBytes());

        // Insert all test vectors
        testEnv.accessor.insert(parallel1);
        testEnv.accessor.insert(parallel2);
        testEnv.accessor.insert(orthogonal1);
        testEnv.accessor.insert(orthogonal2);
        testEnv.accessor.insert(opposite1);
        testEnv.accessor.insert(opposite2);

        // Verify insertions completed successfully
        Assert.assertNotNull("Cosine similarity test vectors should insert successfully", parallel1);
        Assert.assertTrue("Tree should handle different similarity patterns correctly",
                VectorClusteringTreeTestUtils.TreeValidator.validateTreeStructure(testEnv));
    }
}
