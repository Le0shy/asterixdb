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
package org.apache.hyracks.storage.am.lsm.vector.frames;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringNSMFrame;

/**
 * Columnar Vector Clustering Leaf Frame that eliminates the need for separate metadata pages.
 * 
 * This frame directly stores vector data in columnar format with embedded distance range metadata.
 * 
 * Page Layout:
 * +------------------+
 * | Standard Header  |  (page_id, tuple_count, level, etc.)
 * +------------------+
 * | Distance Range   |  (min_distance, max_distance) - replaces metadata pages
 * +------------------+
 * | Column Metadata  |  (column_count, column_offsets[])
 * +------------------+
 * | Distance Column  |  [dist0, dist1, dist2, ...]
 * +------------------+
 * | Vector Columns   |  [dim0: v0,v1,v2...] [dim1: v0,v1,v2...] ... [dimN: v0,v1,v2...]
 * +------------------+
 * | Primary Key Col  |  [pk0, pk1, pk2, ...]
 * +------------------+
 * | Other Columns    |  [field0[]], [field1[]], ...
 * +------------------+
 * 
 * Benefits over metadata pages:
 * 1. Eliminates one level of indirection
 * 2. Better cache locality for distance-based filtering
 * 3. Enables vectorized operations on dimensions
 * 4. Supports efficient range scans
 */
public class ColumnVCTreeLeafFrame extends VectorClusteringNSMFrame {

    // Distance range metadata offsets (embedded in page header)
    public static final int MIN_DISTANCE_OFFSET = CENTROID_DATA_OFFSET;
    public static final int MAX_DISTANCE_OFFSET = MIN_DISTANCE_OFFSET + 4;

    // Column metadata starts after distance range
    public static final int COLUMN_COUNT_OFFSET = MAX_DISTANCE_OFFSET + 4;
    public static final int COLUMN_OFFSETS_START = COLUMN_COUNT_OFFSET + 4;

    // Standard offsets for column layout
    public static final int DISTANCE_COLUMN_INDEX = 0;
    public static final int VECTOR_COLUMNS_START_INDEX = 1;
    // Primary key and other columns follow vector dimensions

    private final int vectorDimensions;
    private final int totalColumns;

    public ColumnVCTreeLeafFrame(ITreeIndexTupleWriter tupleWriter, int vectorDimensions) {
        super(tupleWriter, null, vectorDimensions); // null slotManager for columnar
        this.vectorDimensions = vectorDimensions;
        // Total columns: distance + vector_dims + primary_key + other_fields
        this.totalColumns = 1 + vectorDimensions + 1 + 1; // simplified for now
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);

        // Initialize distance range to invalid values
        buf.putFloat(MIN_DISTANCE_OFFSET, Float.MAX_VALUE);
        buf.putFloat(MAX_DISTANCE_OFFSET, Float.MIN_VALUE);

        // Initialize column metadata
        buf.putInt(COLUMN_COUNT_OFFSET, totalColumns);

        // Initialize column offsets (will be updated as data is added)
        int offsetPos = COLUMN_OFFSETS_START;
        for (int i = 0; i < totalColumns; i++) {
            buf.putInt(offsetPos, -1); // Mark as uninitialized
            offsetPos += 4;
        }
    }

    /**
     * Inserts vector data in columnar format and updates distance range.
     */
    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        // TODO: Extract vector data and store in columnar format
        // Update distance range in page header
        throw new UnsupportedOperationException("Columnar insert implementation pending");
    }

    /**
     * Gets the minimum distance for this page (embedded metadata).
     */
    public float getMinDistance() {
        return buf.getFloat(MIN_DISTANCE_OFFSET);
    }

    /**
     * Gets the maximum distance for this page (embedded metadata).
     */
    public float getMaxDistance() {
        return buf.getFloat(MAX_DISTANCE_OFFSET);
    }

    /**
     * Updates the distance range when new data is inserted.
     */
    public void updateDistanceRange(float distance) {
        float currentMin = getMinDistance();
        float currentMax = getMaxDistance();

        if (distance < currentMin || currentMin == Float.MAX_VALUE) {
            buf.putFloat(MIN_DISTANCE_OFFSET, distance);
        }
        if (distance > currentMax || currentMax == Float.MIN_VALUE) {
            buf.putFloat(MAX_DISTANCE_OFFSET, distance);
        }
    }

    /**
     * Checks if a distance falls within this page's range.
     */
    public boolean containsDistance(float distance) {
        return distance >= getMinDistance() && distance <= getMaxDistance();
    }

    /**
     * Gets the offset for a specific column.
     */
    public int getColumnOffset(int columnIndex) {
        if (columnIndex >= totalColumns) {
            throw new IndexOutOfBoundsException("Column " + columnIndex + " >= " + totalColumns);
        }
        return buf.getInt(COLUMN_OFFSETS_START + columnIndex * 4);
    }

    /**
     * Sets the offset for a specific column.
     */
    public void setColumnOffset(int columnIndex, int offset) {
        if (columnIndex >= totalColumns) {
            throw new IndexOutOfBoundsException("Column " + columnIndex + " >= " + totalColumns);
        }
        buf.putInt(COLUMN_OFFSETS_START + columnIndex * 4, offset);
    }

    /**
     * Creates an iterator for efficient columnar vector similarity search.
     */
    public ColumnarVectorIterator createVectorIterator() {
        return new ColumnarVectorIterator(this, vectorDimensions);
    }

    /**
     * Gets all distances in this page for vectorized operations.
     */
    public float[] getDistanceColumn() {
        int tupleCount = getTupleCount();
        float[] distances = new float[tupleCount];

        int distanceOffset = getColumnOffset(DISTANCE_COLUMN_INDEX);
        for (int i = 0; i < tupleCount; i++) {
            distances[i] = buf.getFloat(distanceOffset + i * 4);
        }
        return distances;
    }

    /**
     * Gets a specific vector dimension column for SIMD operations.
     */
    public float[] getVectorDimensionColumn(int dimension) {
        if (dimension >= vectorDimensions) {
            throw new IndexOutOfBoundsException("Dimension " + dimension + " >= " + vectorDimensions);
        }

        int tupleCount = getTupleCount();
        float[] dimensionValues = new float[tupleCount];

        int columnIndex = VECTOR_COLUMNS_START_INDEX + dimension;
        int columnOffset = getColumnOffset(columnIndex);

        for (int i = 0; i < tupleCount; i++) {
            dimensionValues[i] = buf.getFloat(columnOffset + i * 4);
        }
        return dimensionValues;
    }

    @Override
    public int getPageHeaderSize() {
        // Standard header + distance range + column metadata
        return COLUMN_OFFSETS_START + (totalColumns * 4);
    }

    /**
     * Iterator for efficient columnar vector operations.
     */
    public static class ColumnarVectorIterator {
        private final ColumnVCTreeLeafFrame frame;
        private final int vectorDimensions;
        private int currentIndex;

        public ColumnarVectorIterator(ColumnVCTreeLeafFrame frame, int vectorDimensions) {
            this.frame = frame;
            this.vectorDimensions = vectorDimensions;
            this.currentIndex = 0;
        }

        public boolean hasNext() {
            return currentIndex < frame.getTupleCount();
        }

        public float getDistance() {
            int distanceOffset = frame.getColumnOffset(DISTANCE_COLUMN_INDEX);
            return frame.buf.getFloat(distanceOffset + currentIndex * 4);
        }

        public float getVectorDimension(int dimension) {
            int columnIndex = VECTOR_COLUMNS_START_INDEX + dimension;
            int columnOffset = frame.getColumnOffset(columnIndex);
            return frame.buf.getFloat(columnOffset + currentIndex * 4);
        }

        public void next() {
            currentIndex++;
        }

        public void reset() {
            currentIndex = 0;
        }
    }
}
