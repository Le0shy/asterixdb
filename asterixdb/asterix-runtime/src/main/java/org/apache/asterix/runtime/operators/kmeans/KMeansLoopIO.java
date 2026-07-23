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
package org.apache.asterix.runtime.operators.kmeans;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY Route B (multi-NC systolic exact-init loop) — shared wire formats and raw-vector (de)serialization
 * for the loop's internal edges and run files. These frames never leave the loop sub-graph (WEIGH is fed by the
 * separate {@code KIND_POOL} envelope on Op1's pool output), so they use a compact <b>raw double[]</b> encoding
 * rather than the tagged ordered-list envelope.
 * <p>
 * A vector field is simply its {@code dim} components written back-to-back as raw doubles ({@code dim * 8} bytes);
 * it is read straight off the frame by byte offset ({@link #readRawVector}). The vector column's declared
 * {@link ISerializerDeserializer} in the record descriptors below is therefore a <b>placeholder</b>: every read
 * goes through {@link org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor} field offsets and every write
 * through {@link #writeRawVector}; the serde itself is never invoked, and the broadcast/M-to-1 connectors copy
 * frames byte-for-byte without deserializing.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "CLUSTER BY Route B: shared loop wire formats + raw-vector serde")
public final class KMeansLoopIO {

    private KMeansLoopIO() {
    }

    /** A draw frame carries a real drawn vector; an end-of-round marker closes a round's draw stream. */
    public static final int KIND_DRAW = 0;
    public static final int KIND_END = 1;

    /** Cost -> PhiMerge and PhiMerge -> Sample: {@code {round:int, value:double}} (localSigma, then phi). */
    public static final RecordDescriptor SCALAR_RD = new RecordDescriptor(new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

    /**
     * Sample -> PoolMerge and PoolMerge -> Release: {@code {round:int, part:int, seq:int, kind:int, vec:rawDoubles}}.
     * For {@link #KIND_END} markers, {@code part} identifies the finishing partition and {@code vec}/{@code seq} are
     * ignored. The last column is a raw-double vector (see class comment).
     */
    public static final RecordDescriptor DRAW_RD = new RecordDescriptor(new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE /* placeholder: raw double[] read by offset */ });

    /** The pool run file: one raw-double vector per tuple, {@code {vec:rawDoubles}}. */
    public static final RecordDescriptor POOL_RD = new RecordDescriptor(new ISerializerDeserializer[] {
            DoubleSerializerDeserializer.INSTANCE /* placeholder: raw double[] read by offset */ });

    /** Appends {@code v}'s components as one raw-double field (call after the tuple's earlier fields). */
    public static void writeRawVector(ArrayTupleBuilder tb, double[] v) throws HyracksDataException {
        try {
            DataOutput out = tb.getDataOutput();
            for (double d : v) {
                out.writeDouble(d);
            }
            tb.addFieldEndOffset();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /** Decodes a raw-double vector field ({@code length} bytes = {@code length/8} components) at {@code start}. */
    public static double[] readRawVector(byte[] data, int start, int length) {
        int dim = length / Double.BYTES;
        double[] v = new double[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = DoublePointable.getDouble(data, start + i * Double.BYTES);
        }
        return v;
    }

    /** Sink for {@link #streamRawVectors}: receives each stored vector, may throw on cancellation/error. */
    @FunctionalInterface
    public interface RawVectorConsumer {
        void accept(double[] vec) throws HyracksDataException;
    }

    /**
     * Streams every raw-double vector out of a {@link MaterializerTaskState} run file (pool or resident vectors,
     * both {@link #POOL_RD}) via a fresh reader — repeatable, non-deleting, one frame buffered at a time. Polls
     * the task-thread interrupt per frame so a cancelled job's pure-CPU scan aborts promptly.
     */
    public static void streamRawVectors(MaterializerTaskState state, IHyracksTaskContext ctx, RawVectorConsumer sink)
            throws HyracksDataException {
        FrameTupleAccessor accessor = new FrameTupleAccessor(POOL_RD);
        FrameTupleReference tuple = new FrameTupleReference();
        VSizeFrame frame = new VSizeFrame(ctx);
        RunFileReader reader = state.createReader();
        reader.open();
        try {
            while (reader.nextFrame(frame)) {
                if (Thread.currentThread().isInterrupted()) {
                    throw HyracksDataException.create(new InterruptedException());
                }
                accessor.reset(frame.getBuffer());
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tuple.reset(accessor, i);
                    sink.accept(readRawVector(tuple.getFieldData(0), tuple.getFieldStart(0), tuple.getFieldLength(0)));
                }
            }
        } finally {
            reader.close();
        }
    }

    /** Reads a whole (small) run file — e.g. the candidate pool — into memory. */
    public static List<double[]> readAllRawVectors(MaterializerTaskState state, IHyracksTaskContext ctx)
            throws HyracksDataException {
        List<double[]> out = new ArrayList<>();
        streamRawVectors(state, ctx, out::add);
        return out;
    }

    /** Appends one raw-double vector as a {@link #POOL_RD} tuple into {@code appender} (caller flushes frames). */
    public static void appendPoolVector(ArrayTupleBuilder tb, double[] vec) throws HyracksDataException {
        tb.reset();
        writeRawVector(tb, vec);
    }
}
