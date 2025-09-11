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

package org.apache.hyracks.storage.am.vector;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IIndexTestContext;
import org.apache.hyracks.storage.am.common.TreeIndexTestUtils;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings({ "rawtypes", "deprecation" })
public class VectorTreeTestUtils extends TreeIndexTestUtils {
    private static final Logger LOGGER = LogManager.getLogger();

    // Static initializer for creating predictable test structures
    private static org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer staticInitializer;

    @Override
    protected CheckTuple createCheckTuple(int numFields, int numKeyFields) {
        return new VectorCheckTuple(numFields, numKeyFields);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CheckTuple createIntCheckTuple(int[] fieldValues, int numKeyFields) {
        VectorCheckTuple checkTuple = new VectorCheckTuple(fieldValues.length, numKeyFields);
        for (int v : fieldValues) {
            checkTuple.appendField((Comparable) Integer.valueOf(v));
        }
        return checkTuple;
    }

    @Override
    protected void setIntKeyFields(int[] fieldValues, int numKeyFields, int maxValue, Random rnd) {
        for (int j = 0; j < numKeyFields; j++) {
            fieldValues[j] = rnd.nextInt() % maxValue;
        }
    }

    @Override
    protected void setIntPayloadFields(int[] fieldValues, int numKeyFields, int numFields) {
        for (int j = numKeyFields; j < numFields; j++) {
            fieldValues[j] = j;
        }
    }

    @Override
    protected Collection createCheckTuplesCollection() {
        return new java.util.TreeSet<>();
    }

    @Override
    protected ArrayTupleBuilder createDeleteTupleBuilder(IIndexTestContext ctx) {
        return new ArrayTupleBuilder(ctx.getKeyFieldCount());
    }

    @Override
    protected org.apache.hyracks.storage.common.ISearchPredicate createNullSearchPredicate() {
        return null;
    }

    @Override
    public void checkExpectedResults(IIndexCursor cursor, Collection checkTuples, ISerializerDeserializer[] fieldSerdes,
            int keyFieldCount, Iterator<CheckTuple> checkIter) throws Exception {
        // Implementation will be added based on vector-specific requirements
        throw new UnsupportedOperationException("Vector-specific implementation needed");
    }

    @Override
    protected boolean checkDiskOrderScanResult(ITupleReference tuple, CheckTuple checkTuple, IIndexTestContext ctx) {
        // Vector-specific disk order scan result checking
        // For now, just return true as a placeholder
        return true;
    }

    /**
     * Create a VectorCheckTuple from vector field values
     */
    @SuppressWarnings("unchecked")
    public VectorCheckTuple createVectorCheckTuple(float[][] vectors, String[] metadata, int numKeyFields) {
        VectorCheckTuple checkTuple = new VectorCheckTuple(vectors.length + metadata.length, numKeyFields);

        // Add vector fields - wrap float arrays in a FloatArrayWrapper
        for (float[] vector : vectors) {
            checkTuple.appendField(new VectorCheckTuple.FloatArrayWrapper(vector));
        }

        // Add metadata fields
        for (String meta : metadata) {
            checkTuple.appendField(meta);
        }

        return checkTuple;
    }

    /**
     * Generate random vector tuples and insert them into the index
     */
    @SuppressWarnings("unchecked")
    public void insertVectorTuples(AbstractVectorTreeTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int vectorDimensions = ctx.getVectorDimensions();

        for (int i = 0; i < numTuples; i++) {
            if (LOGGER.isInfoEnabled()) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Vector Tuple " + (i + 1) + "/" + numTuples);
                }
            }

            // Create random vector data
            Object[] fieldValues = new Object[fieldCount];

            // Set vector fields
            for (int j = 0; j < numKeyFields; j++) {
                if (ctx.getFieldSerdes()[j] instanceof FloatArraySerializerDeserializer) {
                    float[] vector = generateRandomVector(vectorDimensions, rnd);
                    fieldValues[j] = vector;
                } else {
                    // String field
                    fieldValues[j] = generateRandomString(5 + rnd.nextInt(10), rnd);
                }
            }

            // Set metadata fields
            for (int j = numKeyFields; j < fieldCount; j++) {
                if (ctx.getFieldSerdes()[j] instanceof FloatArraySerializerDeserializer) {
                    float[] vector = generateRandomVector(vectorDimensions, rnd);
                    fieldValues[j] = vector;
                } else {
                    // String metadata
                    fieldValues[j] = "metadata_" + i + "_" + j;
                }
            }

            // Create tuple and insert
            TupleUtils.createTuple(ctx.getTupleBuilder(), ctx.getTuple(), ctx.getFieldSerdes(), fieldValues);

            try {
                ctx.getIndexAccessor().insert(ctx.getTuple());

                // Create check tuple for validation
                VectorCheckTuple checkTuple = new VectorCheckTuple(fieldCount, numKeyFields);
                for (Object value : fieldValues) {
                    if (value instanceof float[]) {
                        checkTuple.appendField(new VectorCheckTuple.FloatArrayWrapper((float[]) value));
                    } else {
                        checkTuple.appendField((Comparable) value);
                    }
                }
                ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());

            } catch (HyracksDataException e) {
                // Ignore duplicate key insertions
                if (!e.matches(ErrorCode.DUPLICATE_KEY)) {
                    throw e;
                }
            }
        }
    }

    /**
     * Insert mixed vector and string tuples
     */
    public void insertMixedTuples(AbstractVectorTreeTestContext ctx, int numTuples, Random rnd) throws Exception {
        insertVectorTuples(ctx, numTuples, rnd);
    }

    /**
     * Insert edge case vectors (zero vectors, unit vectors, etc.)
     */
    @SuppressWarnings("unchecked")
    public void insertEdgeCaseVectors(AbstractVectorTreeTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int vectorDimensions = ctx.getVectorDimensions();

        for (int i = 0; i < numTuples; i++) {
            Object[] fieldValues = new Object[fieldCount];

            // Set vector fields with edge cases
            for (int j = 0; j < numKeyFields; j++) {
                if (ctx.getFieldSerdes()[j] instanceof FloatArraySerializerDeserializer) {
                    float[] vector;
                    int caseType = i % 4;
                    switch (caseType) {
                        case 0: // Zero vector
                            vector = new float[vectorDimensions];
                            break;
                        case 1: // Unit vector
                            vector = generateUnitVector(vectorDimensions, rnd);
                            break;
                        case 2: // Large values
                            vector = generateLargeVector(vectorDimensions, rnd);
                            break;
                        default: // Small values
                            vector = generateSmallVector(vectorDimensions, rnd);
                            break;
                    }
                    fieldValues[j] = vector;
                } else {
                    fieldValues[j] = "edge_case_" + i + "_" + j;
                }
            }

            // Set metadata fields
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = "edge_metadata_" + i + "_" + j;
            }

            TupleUtils.createTuple(ctx.getTupleBuilder(), ctx.getTuple(), ctx.getFieldSerdes(), fieldValues);

            try {
                ctx.getIndexAccessor().insert(ctx.getTuple());

                VectorCheckTuple checkTuple = new VectorCheckTuple(fieldCount, numKeyFields);
                for (Object value : fieldValues) {
                    if (value instanceof float[]) {
                        checkTuple.appendField(new VectorCheckTuple.FloatArrayWrapper((float[]) value));
                    } else {
                        checkTuple.appendField((Comparable) value);
                    }
                }
                ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());

            } catch (HyracksDataException e) {
                if (!e.matches(ErrorCode.DUPLICATE_KEY)) {
                    throw e;
                }
            }
        }
    }

    /**
     * Placeholder implementations for required abstract methods from TreeIndexTestUtils
     */
    public void checkPointSearches(AbstractVectorTreeTestContext ctx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Point Searches (placeholder).");
        }
        // TODO: Implement vector-specific point searches
    }

    public void checkScan(AbstractVectorTreeTestContext ctx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Scan (placeholder).");
        }
        // TODO: Implement vector-specific scan validation
    }

    public void checkDiskOrderScan(AbstractVectorTreeTestContext ctx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Disk Order Scan (placeholder).");
        }
        // TODO: Implement vector-specific disk order scan
    }

    public void checkRangeSearch(AbstractVectorTreeTestContext ctx, ITupleReference lowKey, ITupleReference highKey,
            boolean lowKeyInclusive, boolean highKeyInclusive) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Range Search (placeholder).");
        }
        // TODO: Implement vector-specific range searches
    }

    public void checkVectorSimilaritySearches(AbstractVectorTreeTestContext ctx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Similarity Searches (placeholder).");
        }
        // TODO: Implement vector similarity searches (k-NN, etc.)
    }

    // Utility methods for vector generation
    private float[] generateRandomVector(int dimensions, Random rnd) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = rnd.nextFloat() * 100.0f - 50.0f; // Range [-50, 50]
        }
        return vector;
    }

    private float[] generateUnitVector(int dimensions, Random rnd) {
        float[] vector = new float[dimensions];
        int nonZeroIndex = rnd.nextInt(dimensions);
        vector[nonZeroIndex] = 1.0f;
        return vector;
    }

    private float[] generateLargeVector(int dimensions, Random rnd) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = rnd.nextFloat() * 10000.0f;
        }
        return vector;
    }

    private float[] generateSmallVector(int dimensions, Random rnd) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = rnd.nextFloat() * 0.01f;
        }
        return vector;
    }

    private String generateRandomString(int length, Random rnd) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = (char) ('a' + rnd.nextInt(26));
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Initialize static tree structure using VectorClusteringTreeStaticInitializer
     */
    public static void initializeStaticStructure(AbstractVectorTreeTestContext ctx,
            org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig config)
            throws Exception {

        // Create test tuples for the structure
        java.util.List<ITupleReference> testTuples = new java.util.ArrayList<>();

        // Generate sample vector tuples for the structure
        // Using fixed seed for predictable results - rnd reserved for future use
        int totalTuples = config.numLeafPages * config.tuplesPerLeaf;

        for (int i = 0; i < totalTuples; i++) {
            Object[] fieldValues = new Object[ctx.getFieldCount()];

            // Set key fields
            for (int j = 0; j < ctx.getKeyFieldCount(); j++) {
                if (ctx.getFieldSerdes()[j] instanceof org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer) {
                    fieldValues[j] = i * 10; // Predictable integer keys
                } else {
                    fieldValues[j] = "key_" + String.format("%03d", i);
                }
            }

            // Set vector and metadata fields
            for (int j = ctx.getKeyFieldCount(); j < ctx.getFieldCount(); j++) {
                if (ctx.getFieldSerdes()[j] instanceof FloatArraySerializerDeserializer) {
                    // Generate predictable vectors based on tuple index
                    float[] vector = generatePredictableVector(4, i); // 4D vectors
                    fieldValues[j] = vector;
                } else {
                    fieldValues[j] = "data_" + i;
                }
            }

            // Create tuple
            TupleUtils.createTuple(ctx.getTupleBuilder(), ctx.getTuple(), ctx.getFieldSerdes(), fieldValues);

            // Add copy of the tuple to test list
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(ctx.getFieldCount());
            ArrayTupleReference tupleRef = new ArrayTupleReference();
            TupleUtils.createTuple(tupleBuilder, tupleRef, ctx.getFieldSerdes(), fieldValues);
            testTuples.add(tupleRef);
        }

        // Initialize the static structure
        org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree vectorTree =
                (org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree) ctx.getIndex();

        org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer initializer =
                new org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer(vectorTree);

        initializer.initializeStaticStructure(config, testTuples);
        staticInitializer = initializer;
    }

    /**
     * Generate predictable vector for testing
     */
    private static float[] generatePredictableVector(int dimensions, int index) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = (float) (index + i * 0.1);
        }
        return vector;
    }

    /**
     * Get the current static initializer
     */
    public static org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer getStaticInitializer() {
        return staticInitializer;
    }

    /**
     * Clean up static initializer
     */
    public static void cleanupStaticInitializer() throws Exception {
        if (staticInitializer != null) {
            staticInitializer.cleanup();
            staticInitializer = null;
        }
    }
}
