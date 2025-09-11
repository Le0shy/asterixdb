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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * Search cursor for LSM Vector Clustering Tree.
 * 
 * This cursor coordinates search operations across multiple LSM components,
 * merging results from memory and disk components while maintaining proper
 * ordering and handling vector similarity search semantics.
 */
public class LSMVCTreeSearchCursor implements IIndexCursor {

    private AbstractLSMIndexOperationContext opCtx;
    private List<ILSMComponent> operationalComponents;
    private List<IIndexCursor> rangeCursors;
    private ISearchPredicate searchPredicate;

    private boolean open;
    private boolean exhausted;
    private ITupleReference currentTuple;
    private int currentCursorIndex;

    public LSMVCTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        this.opCtx = (AbstractLSMIndexOperationContext) opCtx;
        this.open = false;
        this.exhausted = false;
        this.currentCursorIndex = 0;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMVCTreeCursorInitialState lsmInitialState = (LSMVCTreeCursorInitialState) initialState;

        this.searchPredicate = searchPred;
        this.operationalComponents = lsmInitialState.getOperationalComponents();
        this.rangeCursors = lsmInitialState.getCursors();

        this.open = true;
        this.exhausted = false;
        this.currentCursorIndex = 0;
        this.currentTuple = null;

        // Open cursors for all operational components
        for (int i = 0; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);
            IIndexCursor cursor = rangeCursors.get(i);

            if (component instanceof LSMVCTreeMemoryComponent) {
                // Memory component - use VectorClusteringTree accessor
                LSMVCTreeMemoryComponent memComponent = (LSMVCTreeMemoryComponent) component;
                VectorClusteringTree vcTree = memComponent.getIndex();
                IIndexCursor vcCursor = vcTree.createSearchCursor(false);
                rangeCursors.set(i, vcCursor);
                vcCursor.open(null, searchPredicate);
            } else {
                // Disk component - TODO: implement disk component search
                throw new UnsupportedOperationException("Disk component search not yet implemented");
            }
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (!open) {
            return false;
        }

        if (exhausted) {
            return false;
        }

        // Check if current cursor has more results
        if (currentCursorIndex < rangeCursors.size()) {
            IIndexCursor currentCursor = rangeCursors.get(currentCursorIndex);
            if (currentCursor.hasNext()) {
                return true;
            } else {
                // Move to next cursor
                currentCursorIndex++;
                return hasNext();
            }
        }

        // No more cursors
        exhausted = true;
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        if (!hasNext()) {
            throw new IllegalStateException("No more elements");
        }

        IIndexCursor currentCursor = rangeCursors.get(currentCursorIndex);
        currentCursor.next();
        currentTuple = currentCursor.getTuple();
    }

    @Override
    public ITupleReference getTuple() {
        return currentTuple;
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            return;
        }

        // Close all cursors
        if (rangeCursors != null) {
            for (IIndexCursor cursor : rangeCursors) {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }

        open = false;
        exhausted = false;
        currentTuple = null;
        currentCursorIndex = 0;
    }

    @Override
    public void destroy() throws HyracksDataException {
        close();

        // Destroy all cursors
        if (rangeCursors != null) {
            for (IIndexCursor cursor : rangeCursors) {
                if (cursor != null) {
                    cursor.destroy();
                }
            }
            rangeCursors.clear();
        }
    }

    /**
     * Checks if the cursor is currently open.
     */
    public boolean isOpen() {
        return open;
    }

    /**
     * Checks if the cursor is exhausted.
     */
    public boolean isExhausted() {
        return exhausted;
    }
}
