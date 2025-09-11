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
package org.apache.hyracks.storage.am.vector.predicates;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * K-Nearest Neighbor search predicate for vector clustering tree ANN search.
 * Used for efficient k-NN queries with triangle inequality pruning.
 */
public class VectorKNNPredicate implements ISearchPredicate {
    private static final long serialVersionUID = 1L;

    private final float[] queryVector;
    private final int k;
    private final DistanceFunction distanceFunction;

    public enum DistanceFunction {
        EUCLIDEAN,
        COSINE,
        MANHATTAN
    }

    public VectorKNNPredicate(float[] queryVector, int k) {
        this(queryVector, k, DistanceFunction.EUCLIDEAN);
    }

    public VectorKNNPredicate(float[] queryVector, int k, DistanceFunction distanceFunction) {
        if (queryVector == null) {
            throw new IllegalArgumentException("Query vector cannot be null");
        }
        if (k <= 0) {
            throw new IllegalArgumentException("k must be positive");
        }
        this.queryVector = queryVector.clone();
        this.k = k;
        this.distanceFunction = distanceFunction;
    }

    public float[] getQueryVector() {
        return queryVector.clone();
    }

    public int getK() {
        return k;
    }

    public DistanceFunction getDistanceFunction() {
        return distanceFunction;
    }

    @Override
    public MultiComparator getLowKeyComparator() {
        // Vector clustering tree doesn't use traditional key comparisons
        // This method is not applicable for vector searches
        return null;
    }

    @Override
    public MultiComparator getHighKeyComparator() {
        // Vector clustering tree doesn't use traditional key comparisons
        // This method is not applicable for vector searches
        return null;
    }

    @Override
    public ITupleReference getLowKey() {
        // Vector clustering tree doesn't use traditional key searches
        // This method is not applicable for vector searches
        return null;
    }

    public ITupleReference getHighKey() {
        return null;
    }

    public boolean isLowKeyInclusive() {
        return false;
    }

    public boolean isHighKeyInclusive() {
        return false;
    }

    @Override
    public String toString() {
        return "VectorKNNPredicate{" + "queryVector.length=" + queryVector.length + ", k=" + k + ", distanceFunction="
                + distanceFunction + '}';
    }
}
