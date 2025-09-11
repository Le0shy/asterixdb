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
package org.apache.hyracks.storage.am.lsm.vector.writers;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Column writer specialized for vector data in VCTree columnar format.
 * 
 * Optimizes vector storage by:
 * 1. Storing vector dimensions as separate columns for SIMD operations
 * 2. Compressing similar vector values within columns
 * 3. Providing efficient dimension-wise access patterns
 * 4. Supporting vectorized similarity computations
 */
public class VectorColumnWriter {

    private final int vectorDimensions;
    private final int pageSize;
    private final ByteBuffer[] dimensionBuffers;
    private final ByteBuffer distanceBuffer;
    private final ByteBuffer primaryKeyBuffer;

    // Column positions within the page
    private int[] columnOffsets;
    private int[] columnLengths;
    private int tupleCount;

    public VectorColumnWriter(int vectorDimensions, int pageSize) {
        this.vectorDimensions = vectorDimensions;
        this.pageSize = pageSize;

        // Pre-allocate buffers for each vector dimension
        this.dimensionBuffers = new ByteBuffer[vectorDimensions];
        for (int i = 0; i < vectorDimensions; i++) {
            this.dimensionBuffers[i] = ByteBuffer.allocate(pageSize / (vectorDimensions + 3)); // Rough sizing
        }

        this.distanceBuffer = ByteBuffer.allocate(pageSize / (vectorDimensions + 3));
        this.primaryKeyBuffer = ByteBuffer.allocate(pageSize / (vectorDimensions + 3));

        this.columnOffsets = new int[vectorDimensions + 2]; // dimensions + distance + pk
        this.columnLengths = new int[vectorDimensions + 2];
        this.tupleCount = 0;
    }

    /**
     * Writes a vector tuple in columnar format.
     * Tuple format expected: <distance_to_centroid, vector_embedding[0..n], primary_key, other_fields...>
     */
    public void writeVectorTuple(ITupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() < vectorDimensions + 2) {
            throw new IllegalArgumentException("Tuple must have at least " + (vectorDimensions + 2) + " fields");
        }

        // Extract and write distance
        float distance = extractFloat(tuple, 0);
        distanceBuffer.putFloat(distance);

        // Extract and write vector dimensions
        for (int dim = 0; dim < vectorDimensions; dim++) {
            float dimensionValue = extractVectorDimension(tuple, 1, dim);
            dimensionBuffers[dim].putFloat(dimensionValue);
        }

        // Extract and write primary key
        byte[] primaryKey = extractPrimaryKey(tuple, vectorDimensions + 1);
        primaryKeyBuffer.putInt(primaryKey.length);
        primaryKeyBuffer.put(primaryKey);

        tupleCount++;
    }

    /**
     * Flushes all column data to the target page buffer.
     * 
     * Page layout:
     * [Header][Column_Offsets][Distance_Column][Vector_Dim0][Vector_Dim1]...[Vector_DimN][PrimaryKey_Column]
     */
    public void flush(ByteBuffer pageBuffer) throws HyracksDataException {
        pageBuffer.clear();

        // Calculate column offsets
        int currentOffset = calculateHeaderSize();

        // Write distance column
        columnOffsets[0] = currentOffset;
        distanceBuffer.flip();
        columnLengths[0] = distanceBuffer.remaining();
        currentOffset += columnLengths[0];

        // Write vector dimension columns
        for (int dim = 0; dim < vectorDimensions; dim++) {
            columnOffsets[dim + 1] = currentOffset;
            dimensionBuffers[dim].flip();
            columnLengths[dim + 1] = dimensionBuffers[dim].remaining();
            currentOffset += columnLengths[dim + 1];
        }

        // Write primary key column
        columnOffsets[vectorDimensions + 1] = currentOffset;
        primaryKeyBuffer.flip();
        columnLengths[vectorDimensions + 1] = primaryKeyBuffer.remaining();

        // Write header with column metadata
        writeHeader(pageBuffer);

        // Write column data
        pageBuffer.put(distanceBuffer);
        for (int dim = 0; dim < vectorDimensions; dim++) {
            pageBuffer.put(dimensionBuffers[dim]);
        }
        pageBuffer.put(primaryKeyBuffer);
    }

    private void writeHeader(ByteBuffer pageBuffer) {
        // Write tuple count
        pageBuffer.putInt(tupleCount);

        // Write vector dimensions
        pageBuffer.putInt(vectorDimensions);

        // Write column offsets
        for (int i = 0; i < columnOffsets.length; i++) {
            pageBuffer.putInt(columnOffsets[i]);
            pageBuffer.putInt(columnLengths[i]);
        }
    }

    private int calculateHeaderSize() {
        // tuple_count(4) + vector_dimensions(4) + (offset+length)(8) * num_columns
        return 8 + (8 * (vectorDimensions + 2));
    }

    private float extractFloat(ITupleReference tuple, int fieldIndex) throws HyracksDataException {
        byte[] data = tuple.getFieldData(fieldIndex);
        int offset = tuple.getFieldStart(fieldIndex);
        return ByteBuffer.wrap(data, offset, 4).getFloat();
    }

    private float extractVectorDimension(ITupleReference tuple, int vectorFieldIndex, int dimension)
            throws HyracksDataException {
        byte[] vectorData = tuple.getFieldData(vectorFieldIndex);
        int vectorStart = tuple.getFieldStart(vectorFieldIndex);

        // Assume vector is stored as float array
        int dimensionOffset = vectorStart + (dimension * 4);
        return ByteBuffer.wrap(vectorData, dimensionOffset, 4).getFloat();
    }

    private byte[] extractPrimaryKey(ITupleReference tuple, int fieldIndex) throws HyracksDataException {
        byte[] data = tuple.getFieldData(fieldIndex);
        int start = tuple.getFieldStart(fieldIndex);
        int length = tuple.getFieldLength(fieldIndex);

        byte[] primaryKey = new byte[length];
        System.arraycopy(data, start, primaryKey, 0, length);
        return primaryKey;
    }

    /**
     * Resets the writer for reuse with a new page.
     */
    public void reset() {
        for (ByteBuffer buffer : dimensionBuffers) {
            buffer.clear();
        }
        distanceBuffer.clear();
        primaryKeyBuffer.clear();
        tupleCount = 0;
    }

    public int getTupleCount() {
        return tupleCount;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }

    /**
     * Estimates the space required for the current data.
     */
    public int getEstimatedSize() {
        int size = calculateHeaderSize();
        size += distanceBuffer.position();
        for (ByteBuffer buffer : dimensionBuffers) {
            size += buffer.position();
        }
        size += primaryKeyBuffer.position();
        return size;
    }

    /**
     * Checks if adding another tuple would exceed page capacity.
     */
    public boolean hasSpaceForTuple(ITupleReference tuple) {
        int additionalSize = 4; // distance
        additionalSize += vectorDimensions * 4; // vector dimensions
        additionalSize += 4 + tuple.getFieldLength(vectorDimensions + 1); // primary key length + data

        return getEstimatedSize() + additionalSize <= pageSize;
    }
}
