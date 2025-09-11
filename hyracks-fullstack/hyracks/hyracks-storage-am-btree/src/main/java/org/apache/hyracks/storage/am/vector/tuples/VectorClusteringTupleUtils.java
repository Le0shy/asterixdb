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

package org.apache.hyracks.storage.am.vector.tuples;

import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleReference;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;

/**
 * Utility class for vector clustering tuple operations.
 * Provides common functionality for tuple copying, field extraction, and manipulation.
 */
public class VectorClusteringTupleUtils {

    /**
     * Copy tuple data to a new SimpleTupleReference using TupleUtils.
     * 
     * @param source The source tuple to copy
     * @param target The target SimpleTupleReference to copy to
     * @throws HyracksDataException if copying fails
     */
    public static void copyTuple(ITupleReference source, SimpleTupleReference target) throws HyracksDataException {
        // Use TupleUtils for consistent tuple copying
        ITupleReference copiedTuple = TupleUtils.copyTuple(source);
        target.setFieldCount(copiedTuple.getFieldCount());
        target.resetByTupleOffset(copiedTuple.getFieldData(0), 0);
    }

    /**
     * Extract primary key from tuple (assumes last field).
     * 
     * @param tuple The tuple to extract primary key from
     * @return Byte array containing the primary key, or null if extraction fails
     */
    public static byte[] extractPrimaryKeyFromTuple(ITupleReference tuple) {
        if (tuple == null || tuple.getFieldCount() == 0) {
            System.out.println("DEBUG: extractPrimaryKeyFromTuple - tuple is null or empty");
            return null;
        }

        int pkFieldIndex = tuple.getFieldCount() - 1;
        System.out.println("DEBUG: extractPrimaryKeyFromTuple - fieldCount=" + tuple.getFieldCount() + ", pkFieldIndex="
                + pkFieldIndex);

        byte[] data = tuple.getFieldData(pkFieldIndex);
        if (data == null) {
            System.out.println("DEBUG: extractPrimaryKeyFromTuple - data is null");
            return null;
        }

        int offset = tuple.getFieldStart(pkFieldIndex);
        int length = tuple.getFieldLength(pkFieldIndex);

        System.out.println("DEBUG: extractPrimaryKeyFromTuple - offset=" + offset + ", length=" + length
                + ", data.length=" + data.length);

        if (length <= 0) {
            System.out.println("DEBUG: extractPrimaryKeyFromTuple - length <= 0");
            return null;
        }

        try {
            byte[] result = Arrays.copyOfRange(data, offset, offset + length);
            System.out.println("DEBUG: extractPrimaryKeyFromTuple - extracted PK: " + Arrays.toString(result));
            return result;
        } catch (Exception e) {
            System.err.println("ERROR: Failed to extract primary key from tuple: " + e.getMessage());
            return null;
        }
    }

    /**
     * Extract vector from tuple.
     * 
     * @param tuple The tuple to extract vector from
     * @return Float array containing the vector, or null if extraction fails
     */
    public static float[] extractVectorFromTuple(ITupleReference tuple) {
        if (tuple == null) {
            return null;
        }

        // Check if tuple has at least 2 fields (for input tuples with vector + PK)
        if (tuple.getFieldCount() < 2) {
            return null;
        }

        // Determine the vector field index based on tuple type:
        // 1. Data tuples: <distance, cosine, vector, PK> - vector is in field 2
        // 2. Input tuples: <vector, PK> - vector is in field 0
        // 3. Update tuples with included fields: <vector, included_field1, ..., included_fieldN, PK> - vector is in field 0

        int vectorFieldIndex;
        if (tuple.getFieldCount() == 4) {
            // Could be either data tuple or update tuple with 2 included fields
            // Try to distinguish by attempting vector extraction from field 0 first
            byte[] field0Data = tuple.getFieldData(0);
            if (field0Data != null && field0Data.length > 0) {
                int field0Length = tuple.getFieldLength(0);
                // Check if field 0 looks like vector data (length multiple of 4)
                if (field0Length % 4 == 0 && field0Length >= 4) {
                    vectorFieldIndex = 0; // Update tuple with included fields
                } else {
                    vectorFieldIndex = 2; // Data tuple
                }
            } else {
                vectorFieldIndex = 2; // Default to data tuple if field 0 is null/empty
            }
        } else {
            // For all other cases (2 fields, 3 fields, 5+ fields), vector is at index 0
            vectorFieldIndex = 0;
        }

        byte[] data = tuple.getFieldData(vectorFieldIndex);
        if (data == null) {
            return null;
        }

        int offset = tuple.getFieldStart(vectorFieldIndex);
        int length = tuple.getFieldLength(vectorFieldIndex);

        if (length <= 0) {
            return null;
        }

        try {
            return VectorUtils.bytesToFloatArray(Arrays.copyOfRange(data, offset, offset + length));
        } catch (Exception e) {
            // Log the error and return null instead of crashing
            System.err.println("ERROR: Failed to extract vector from tuple: " + e.getMessage());
            return null;
        }
    }
}
