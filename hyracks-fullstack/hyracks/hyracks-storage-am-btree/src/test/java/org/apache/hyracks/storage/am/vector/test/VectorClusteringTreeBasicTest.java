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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic unit test for VectorClusteringTree operations without complex mocking.
 * This test focuses on core functionality that can be tested without full tree initialization.
 */
public class VectorClusteringTreeBasicTest {

    private static final int VECTOR_DIMENSIONS = 128;
    private static final int NUM_TEST_RECORDS = 10;
    private static final Random random = new Random(42); // Fixed seed for reproducible tests

    private List<ITupleReference> testTuples;
    private List<double[]> testVectors;
    private IBinaryComparatorFactory[] comparatorFactories;

    @Before
    public void setUp() throws HyracksDataException {
        // Initialize test data structures
        testTuples = new ArrayList<>();
        testVectors = new ArrayList<>();

        // Setup comparator factories
        comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;

        // Generate test data
        generateTestData();
    }

    private void generateTestData() throws HyracksDataException {
        // Generate test vectors and corresponding tuples
        for (int i = 0; i < NUM_TEST_RECORDS; i++) {
            // Generate random vector
            double[] vector = generateRandomVector(VECTOR_DIMENSIONS);
            testVectors.add(vector);

            // Create tuple with record ID and vector
            ITupleReference tuple = createVectorTuple(i, vector);
            testTuples.add(tuple);
        }

        System.out.println("Generated " + NUM_TEST_RECORDS + " test vectors with " + VECTOR_DIMENSIONS + " dimensions");
    }

    private double[] generateRandomVector(int dimensions) {
        double[] vector = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = random.nextGaussian(); // Normal distribution for realistic vectors
        }
        return vector;
    }

    private ITupleReference createVectorTuple(int recordId, double[] vector) throws HyracksDataException {
        try {
            // Create tuple with record ID and vector using TupleUtils
            ByteArrayOutputStream vectorBytes = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(vectorBytes);

            // Write vector length
            dos.writeInt(vector.length);
            // Write vector components
            for (double component : vector) {
                dos.writeFloat((float) component);
            }
            dos.flush();

            // Create tuple using modern TupleUtils approach with proper serializers
            return TupleUtils.createTuple(
                    new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                            ByteArraySerializerDeserializer.INSTANCE },
                    new Object[] { recordId, vectorBytes.toByteArray() });

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Test basic tuple creation and data integrity
     */
    @Test
    public void testTupleCreation() throws HyracksDataException {
        System.out.println("Testing tuple creation and data integrity...");

        // Test that all tuples were created successfully
        Assert.assertEquals("Should have created correct number of tuples", NUM_TEST_RECORDS, testTuples.size());
        Assert.assertEquals("Should have created correct number of vectors", NUM_TEST_RECORDS, testVectors.size());

        // Test tuple structure
        for (int i = 0; i < testTuples.size(); i++) {
            ITupleReference tuple = testTuples.get(i);

            // Verify tuple has correct number of fields
            Assert.assertEquals("Tuple should have 2 fields", 2, tuple.getFieldCount());

            // Verify record ID field exists and has data
            Assert.assertTrue("Record ID field should have data", tuple.getFieldLength(0) > 0);

            // Verify vector field exists and has data
            Assert.assertTrue("Vector field should have data", tuple.getFieldLength(1) > 0);

            // The vector field should be at least 4 bytes (for dimension count) + dimensions * 4 bytes
            int expectedMinSize = 4 + VECTOR_DIMENSIONS * 4;
            Assert.assertTrue("Vector field should have expected minimum size",
                    tuple.getFieldLength(1) >= expectedMinSize);
        }

        System.out.println("Successfully validated tuple creation for " + testTuples.size() + " tuples");
    }

    /**
     * Test distance calculation functionality using VectorUtils
     */
    @Test
    public void testDistanceCalculations() throws HyracksDataException {
        System.out.println("Testing distance calculations...");

        // Create test vectors with known distances
        double[] centerVector = new double[VECTOR_DIMENSIONS];
        // Center vector is all zeros

        double[] nearVector = new double[VECTOR_DIMENSIONS];
        for (int i = 0; i < VECTOR_DIMENSIONS; i++) {
            nearVector[i] = 0.1; // Close to center
        }

        double[] farVector = new double[VECTOR_DIMENSIONS];
        for (int i = 0; i < VECTOR_DIMENSIONS; i++) {
            farVector[i] = 10.0; // Far from center
        }

        // Test Euclidean distance
        double nearEuclideanDistance = VectorUtils.calculateEuclideanDistance(convertToFloatArray(centerVector),
                convertToFloatArray(nearVector));
        double farEuclideanDistance = VectorUtils.calculateEuclideanDistance(convertToFloatArray(centerVector),
                convertToFloatArray(farVector));

        Assert.assertTrue("Near vector should be closer than far vector (Euclidean)",
                nearEuclideanDistance < farEuclideanDistance);

        // Test Manhattan distance
        double nearManhattanDistance = VectorUtils.calculateManhattanDistance(convertToFloatArray(centerVector),
                convertToFloatArray(nearVector));
        double farManhattanDistance = VectorUtils.calculateManhattanDistance(convertToFloatArray(centerVector),
                convertToFloatArray(farVector));

        Assert.assertTrue("Near vector should be closer than far vector (Manhattan)",
                nearManhattanDistance < farManhattanDistance);

        // Test cosine similarity
        double nearCosineSimilarity = VectorUtils.calculateCosineSimilarity(convertToFloatArray(centerVector),
                convertToFloatArray(nearVector));
        double farCosineSimilarity = VectorUtils.calculateCosineSimilarity(convertToFloatArray(centerVector),
                convertToFloatArray(farVector));

        // Since center vector is all zeros, cosine similarity should be 0 for both
        Assert.assertEquals("Cosine similarity with zero vector should be 0", 0.0, nearCosineSimilarity, 0.001);
        Assert.assertEquals("Cosine similarity with zero vector should be 0", 0.0, farCosineSimilarity, 0.001);

        // Test mixed parameter types
        double mixedEuclideanDistance =
                VectorUtils.calculateEuclideanDistance(convertToFloatArray(nearVector), farVector);
        Assert.assertTrue("Mixed parameter distance should be positive", mixedEuclideanDistance > 0);

        System.out.println("Distance calculation tests completed successfully");
        System.out.println("Near Euclidean distance: " + nearEuclideanDistance);
        System.out.println("Far Euclidean distance: " + farEuclideanDistance);
        System.out.println("Mixed parameter distance: " + mixedEuclideanDistance);
    }

    /**
     * Test vector normalization and quantization
     */
    @Test
    public void testVectorOperations() throws HyracksDataException {
        System.out.println("Testing vector operations...");

        // Test vector normalization
        double[] testVector = { 3.0, 4.0, 0.0 }; // Length should be 5.0
        float[] normalizedVector = VectorUtils.normalizeVector(convertToFloatArray(testVector));

        // Check that normalized vector has unit length
        double normalizedLength = 0.0;
        for (float component : normalizedVector) {
            normalizedLength += component * component;
        }
        normalizedLength = Math.sqrt(normalizedLength);

        Assert.assertEquals("Normalized vector should have unit length", 1.0, normalizedLength, 0.001);

        // Test binary quantization
        double[] binaryTestVector = { 1.0, -1.0, 0.5, -0.5, 0.0 };
        byte[] quantized = VectorUtils.binaryQuantize(convertToFloatArray(binaryTestVector));
        float[] dequantized = VectorUtils.binaryDequantize(quantized, binaryTestVector.length);

        // Verify quantization results
        Assert.assertEquals("Quantized vector should have correct length", binaryTestVector.length, dequantized.length);
        Assert.assertEquals("Positive value should become +1.0", 1.0f, dequantized[0], 0.001f);
        Assert.assertEquals("Negative value should become -1.0", -1.0f, dequantized[1], 0.001f);
        Assert.assertEquals("Positive value should become +1.0", 1.0f, dequantized[2], 0.001f);
        Assert.assertEquals("Negative value should become -1.0", -1.0f, dequantized[3], 0.001f);
        Assert.assertEquals("Zero should become -1.0", -1.0f, dequantized[4], 0.001f);

        System.out.println("Vector operations tests completed successfully");
    }

    /**
     * Test vector serialization and deserialization
     */
    @Test
    public void testVectorSerialization() throws HyracksDataException {
        System.out.println("Testing vector serialization...");

        double[] originalVector = { 1.5, -2.3, 0.0, 3.7, -0.1 };
        float[] floatVector = convertToFloatArray(originalVector);

        // Test float array to bytes conversion
        byte[] serialized = VectorUtils.floatArrayToBytes(floatVector);
        float[] deserialized = VectorUtils.bytesToFloatArray(serialized);

        // Verify serialization/deserialization
        Assert.assertEquals("Deserialized vector should have same length", floatVector.length, deserialized.length);

        for (int i = 0; i < floatVector.length; i++) {
            Assert.assertEquals("Component " + i + " should match after serialization", floatVector[i], deserialized[i],
                    0.001f);
        }

        System.out.println("Vector serialization tests completed successfully");
    }

    /**
     * Test comparator functionality
     */
    @Test
    public void testComparators() throws HyracksDataException {
        System.out.println("Testing comparator functionality...");

        // Test that comparator factories are properly configured
        Assert.assertNotNull("Comparator factories should not be null", comparatorFactories);
        Assert.assertEquals("Should have one comparator factory", 1, comparatorFactories.length);
        Assert.assertNotNull("Integer comparator factory should not be null", comparatorFactories[0]);

        // Test comparator creation
        try {
            Assert.assertNotNull("Should be able to create comparator",
                    comparatorFactories[0].createBinaryComparator());
        } catch (Exception e) {
            Assert.fail("Should be able to create binary comparator: " + e.getMessage());
        }

        System.out.println("Comparator tests completed successfully");
    }

    /**
     * Test centroid calculation
     */
    @Test
    public void testCentroidCalculation() throws HyracksDataException {
        System.out.println("Testing centroid calculation...");

        // Create test vectors for centroid calculation
        float[][] vectors = { { 1.0f, 2.0f, 3.0f }, { 2.0f, 4.0f, 6.0f }, { 3.0f, 6.0f, 9.0f } };

        float[] centroid = VectorUtils.computeCentroid(vectors);

        // Expected centroid should be (2.0, 4.0, 6.0)
        Assert.assertEquals("Centroid should have correct dimensions", 3, centroid.length);
        Assert.assertEquals("Centroid X component", 2.0f, centroid[0], 0.001f);
        Assert.assertEquals("Centroid Y component", 4.0f, centroid[1], 0.001f);
        Assert.assertEquals("Centroid Z component", 6.0f, centroid[2], 0.001f);

        System.out.println("Centroid calculation tests completed successfully");
    }

    // Helper methods
    private float[] convertToFloatArray(double[] doubleArray) {
        float[] floatArray = new float[doubleArray.length];
        for (int i = 0; i < doubleArray.length; i++) {
            floatArray[i] = (float) doubleArray[i];
        }
        return floatArray;
    }
}
