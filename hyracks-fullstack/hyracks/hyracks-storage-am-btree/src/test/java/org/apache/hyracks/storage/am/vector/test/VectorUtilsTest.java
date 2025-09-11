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

import static org.junit.Assert.*;

import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.junit.Test;

/**
 * Unit tests for VectorUtils utility class.
 */
public class VectorUtilsTest {

    private static final float TOLERANCE = 0.001f;

    @Test
    public void testEuclideanDistance() {
        float[] vector1 = { 1.0f, 2.0f, 3.0f };
        float[] vector2 = { 4.0f, 5.0f, 6.0f };

        double distance = VectorUtils.calculateEuclideanDistance(vector1, vector2);
        double expected = Math.sqrt(27.0); // sqrt((4-1)^2 + (5-2)^2 + (6-3)^2)

        assertEquals(expected, distance, TOLERANCE);
    }

    @Test
    public void testCosineSimilarity() {
        // Test perpendicular vectors
        float[] vector1 = { 1.0f, 0.0f, 0.0f };
        float[] vector2 = { 0.0f, 1.0f, 0.0f };

        double similarity = VectorUtils.calculateCosineSimilarity(vector1, vector2);
        assertEquals(0.0, similarity, TOLERANCE);

        // Test identical vectors
        float[] vector3 = { 1.0f, 1.0f, 1.0f };
        float[] vector4 = { 1.0f, 1.0f, 1.0f };

        similarity = VectorUtils.calculateCosineSimilarity(vector3, vector4);
        assertEquals(1.0, similarity, TOLERANCE);

        // Test opposite vectors
        float[] vector5 = { 1.0f, 1.0f, 1.0f };
        float[] vector6 = { -1.0f, -1.0f, -1.0f };

        similarity = VectorUtils.calculateCosineSimilarity(vector5, vector6);
        assertEquals(-1.0, similarity, TOLERANCE);
    }

    @Test
    public void testManhattanDistance() {
        float[] vector1 = { 1.0f, 2.0f, 3.0f };
        float[] vector2 = { 4.0f, 5.0f, 6.0f };

        double distance = VectorUtils.calculateManhattanDistance(vector1, vector2);
        double expected = 9.0; // |4-1| + |5-2| + |6-3|

        assertEquals(expected, distance, TOLERANCE);
    }

    @Test
    public void testNormalizeVector() {
        float[] vector = { 3.0f, 4.0f, 0.0f };
        float[] normalized = VectorUtils.normalizeVector(vector);

        // Expected: norm = sqrt(9 + 16) = 5
        // Normalized: {0.6, 0.8, 0.0}
        assertEquals(0.6f, normalized[0], TOLERANCE);
        assertEquals(0.8f, normalized[1], TOLERANCE);
        assertEquals(0.0f, normalized[2], TOLERANCE);

        // Verify unit length
        double norm = Math
                .sqrt(normalized[0] * normalized[0] + normalized[1] * normalized[1] + normalized[2] * normalized[2]);
        assertEquals(1.0, norm, TOLERANCE);
    }

    @Test
    public void testBinaryQuantization() {
        float[] vector = { 1.5f, -2.0f, 0.0f, 3.5f, -1.0f, 2.0f, 0.5f, -0.5f };
        byte[] quantized = VectorUtils.binaryQuantize(vector);

        // Expected bits: 10010110 (1 for positive, 0 for negative/zero)
        // In byte form: 0x96 = 150
        assertEquals(1, quantized.length);
        assertEquals((byte) 0x96, quantized[0]);

        // Test dequantization
        float[] dequantized = VectorUtils.binaryDequantize(quantized, 8);
        float[] expected = { 1.0f, -1.0f, -1.0f, 1.0f, -1.0f, 1.0f, 1.0f, -1.0f };

        assertArrayEquals(expected, dequantized, TOLERANCE);
    }

    @Test
    public void testScalarQuantization() {
        float[] vector = { 1.0f, 2.5f, 4.0f, 0.5f };
        float minValue = 0.0f;
        float maxValue = 4.0f;
        int bitsPerDimension = 2; // 4 levels: 0, 1, 2, 3

        byte[] quantized = VectorUtils.scalarQuantize(vector, bitsPerDimension, minValue, maxValue);

        // Expected quantized values:
        // 1.0 -> level 1 (roughly)
        // 2.5 -> level 2 (roughly)  
        // 4.0 -> level 3
        // 0.5 -> level 0 (roughly)
        assertEquals(4, quantized.length);

        // Test dequantization
        float[] dequantized = VectorUtils.scalarDequantize(quantized, bitsPerDimension, minValue, maxValue);
        assertEquals(4, dequantized.length);

        // Values should be close to original but quantized to levels
        for (int i = 0; i < vector.length; i++) {
            assertTrue("Dequantized value should be within range",
                    dequantized[i] >= minValue && dequantized[i] <= maxValue);
        }
    }

    @Test
    public void testComputeCentroid() {
        float[][] vectors = { { 1.0f, 2.0f, 3.0f }, { 4.0f, 5.0f, 6.0f }, { 7.0f, 8.0f, 9.0f } };

        float[] centroid = VectorUtils.computeCentroid(vectors);

        // Expected centroid: {4.0, 5.0, 6.0}
        assertEquals(3, centroid.length);
        assertEquals(4.0f, centroid[0], TOLERANCE);
        assertEquals(5.0f, centroid[1], TOLERANCE);
        assertEquals(6.0f, centroid[2], TOLERANCE);
    }

    @Test
    public void testApproximatelyEqual() {
        float[] vector1 = { 1.0f, 2.0f, 3.0f };
        float[] vector2 = { 1.001f, 2.001f, 3.001f };
        float[] vector3 = { 1.1f, 2.1f, 3.1f };

        assertTrue(VectorUtils.approximatelyEqual(vector1, vector2, 0.01f));
        assertFalse(VectorUtils.approximatelyEqual(vector1, vector3, 0.01f));
        assertTrue(VectorUtils.approximatelyEqual(vector1, vector3, 0.2f));
    }

    @Test
    public void testFloatArraySerialization() {
        float[] original = { 1.5f, -2.0f, 3.14159f, 0.0f };

        byte[] bytes = VectorUtils.floatArrayToBytes(original);
        assertEquals(16, bytes.length); // 4 floats * 4 bytes each

        float[] deserialized = VectorUtils.bytesToFloatArray(bytes);

        assertArrayEquals(original, deserialized, TOLERANCE);
    }

    @Test
    public void testApproximateDistanceBinary() {
        // Create two binary quantized vectors
        byte[] quantized1 = { (byte) 0b10101010 }; // 8 dimensions
        byte[] quantized2 = { (byte) 0b11110000 }; // 8 dimensions

        double distance = VectorUtils.approximateDistanceBinary(quantized1, quantized2, 8);

        // Hamming distance should be 4 (4 different bits)
        // Approximate Euclidean distance should be sqrt(4) = 2.0
        assertEquals(2.0, distance, TOLERANCE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEuclideanDistanceDifferentDimensions() {
        float[] vector1 = { 1.0f, 2.0f };
        float[] vector2 = { 1.0f, 2.0f, 3.0f };

        VectorUtils.calculateEuclideanDistance(vector1, vector2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCosineSimilarityDifferentDimensions() {
        float[] vector1 = { 1.0f, 2.0f };
        float[] vector2 = { 1.0f, 2.0f, 3.0f };

        VectorUtils.calculateCosineSimilarity(vector1, vector2);
    }
}
