/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class AsyncSnapshotWriterImpl implements AsyncSnapshotWriter {

    private static final int MAX_CHUNK_SIZE = 128 * 1024;
    private static final int COMPRESSION_BUFFER_SIZE = 4096;
    private static final float COMPRESSION_FACTOR_LOWER_BOUND = 0.4f;

    final int usableChunkSize; // this includes the serialization header for byte[], but not the terminator
    final byte[] serializedByteArrayHeader = new byte[3 * Bits.INT_SIZE_IN_BYTES];
    final byte[] valueTerminator;
    final AtomicInteger numConcurrentAsyncOps;

    private final IPartitionService partitionService;

    private final CustomByteArrayOutputStream[] buffers;
    private final CustomByteArrayOutputStream compressionBuffer = new CustomByteArrayOutputStream(Integer.MAX_VALUE);
    private final int[] partitionKeys;
    private final int[] partitionSequences;
    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final boolean useBigEndian;
    private IMap<SnapshotDataKey, byte[]> currentMap;
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();
    private final AtomicInteger numActiveFlushes = new AtomicInteger();

    private final ExecutionCallback<Void> callback = new ExecutionCallback<Void>() {
        @Override
        public void onResponse(Void response) {
            numActiveFlushes.decrementAndGet();
            numConcurrentAsyncOps.decrementAndGet();
        }

        @Override
        public void onFailure(Throwable t) {
            logger.severe("Error writing to snapshot map '" + currentMap.getName() + "'", t);
            lastError.compareAndSet(null, t);
            numActiveFlushes.decrementAndGet();
            numConcurrentAsyncOps.decrementAndGet();
        }
    };

    public AsyncSnapshotWriterImpl(NodeEngine nodeEngine) {
        this(MAX_CHUNK_SIZE, nodeEngine);
    }

    // for test
    AsyncSnapshotWriterImpl(int chunkSize, NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());

        useBigEndian = !nodeEngine.getHazelcastInstance().getConfig().getSerializationConfig().isUseNativeByteOrder()
                || ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

        Bits.writeInt(serializedByteArrayHeader, Bits.INT_SIZE_IN_BYTES, SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY,
                useBigEndian);

        buffers = new CustomByteArrayOutputStream[partitionService.getPartitionCount()];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = new CustomByteArrayOutputStream(chunkSize);
            buffers[i].write(serializedByteArrayHeader, 0, serializedByteArrayHeader.length);
        }

        JetService jetService = nodeEngine.getService(JetService.SERVICE_NAME);
        this.partitionKeys = jetService.getSharedPartitionKeys();
        this.partitionSequences = new int[partitionService.getPartitionCount()];

        this.numConcurrentAsyncOps = jetService.numConcurrentAsyncOps();

        byte[] valueTerminatorWithHeader = nodeEngine.getSerializationService().toData(
                SnapshotDataValueTerminator.INSTANCE).toByteArray();
        valueTerminator = Arrays.copyOfRange(valueTerminatorWithHeader, HeapData.TYPE_OFFSET,
                valueTerminatorWithHeader.length);
        usableChunkSize = chunkSize - valueTerminator.length;
    }

    @Override
    public void setCurrentMap(String mapName) {
        assert isEmpty() : "writer not empty";
        currentMap = nodeEngine.getHazelcastInstance().getMap(mapName);
    }

    @Override
    @CheckReturnValue
    public boolean offer(Entry<? extends Data, ? extends Data> entry) {
        int partitionId = partitionService.getPartitionId(entry.getKey());
        int length = entry.getKey().totalSize() + entry.getValue().totalSize() - 2 * HeapData.TYPE_OFFSET;

        // if single entry is larger than usableChunkSize, send it alone. We avoid adding it to the ByteArrayOutputStream,
        // since it will grow beyond maximum capacity and never shrink again.
        if (length > usableChunkSize) {
            return putAsyncToMap(partitionId, () -> {
                // 40% reduction by compression is a defensive estimate - to avoid reallocation
                ByteArrayOutputStream baos = new ByteArrayOutputStream((int) (length * COMPRESSION_FACTOR_LOWER_BOUND));
                baos.write(serializedByteArrayHeader, 0, serializedByteArrayHeader.length);
                DeflaterOutputStream dos = new DeflaterOutputStream(baos);
                writeWithoutHeader(entry.getKey(), dos);
                writeWithoutHeader(entry.getValue(), dos);
                try {
                    dos.write(valueTerminator);
                    dos.close();
                } catch (IOException e) {
                    throw new RuntimeException(e); // should never happen
                }

                byte[] data = baos.toByteArray();
                updateSerializedBytesLength(data);
                return new HeapData(data);
            });
        }

        // if the buffer will exceed usableChunkSize after adding this entry, flush it first
        if (buffers[partitionId].size() + length > usableChunkSize && !flush(partitionId)) {
            return false;
        }

        // append to buffer
        writeWithoutHeader(entry.getKey(), buffers[partitionId]);
        writeWithoutHeader(entry.getValue(), buffers[partitionId]);
        return true;
    }

    private void writeWithoutHeader(Data src, OutputStream dst) {
        byte[] bytes = src.toByteArray();
        try {
            dst.write(bytes, HeapData.TYPE_OFFSET, bytes.length - HeapData.TYPE_OFFSET);
        } catch (IOException e) {
            throw new RuntimeException(e); // should never happen
        }
    }

    @CheckReturnValue
    private boolean flush(int partitionId) {
        return containsOnlyHeader(buffers[partitionId])
                || putAsyncToMap(partitionId, () -> getBufferContentsAndClear(buffers[partitionId]));
    }

    private boolean containsOnlyHeader(CustomByteArrayOutputStream buffer) {
        return buffer.size() == serializedByteArrayHeader.length;
    }

    private Data getBufferContentsAndClear(CustomByteArrayOutputStream buffer) {
        buffer.write(valueTerminator, 0, valueTerminator.length);

        final byte[] data;
        // we compress only the buffer payload (including the terminator)
        compressionBuffer.write(serializedByteArrayHeader, 0, serializedByteArrayHeader.length);
        Deflater compressor = new Deflater(Deflater.BEST_SPEED);
        compressor.setInput(buffer.getInternalArray(), serializedByteArrayHeader.length,
                buffer.size() - serializedByteArrayHeader.length);
        compressor.finish();
        byte[] buf = new byte[COMPRESSION_BUFFER_SIZE];
        while (!compressor.finished()) {
            int count = compressor.deflate(buf);
            compressionBuffer.write(buf, 0, count);
        }

        data = Arrays.copyOf(compressionBuffer.getInternalArray(), compressionBuffer.size());
        compressionBuffer.reset();

        updateSerializedBytesLength(data);

        buffer.reset();
        buffer.write(serializedByteArrayHeader, 0, serializedByteArrayHeader.length);
        return new HeapData(data);
    }

    private void updateSerializedBytesLength(byte[] data) {
        // update the array length at the beginning of the buffer
        // the length is the third int value in the serialized data
        Bits.writeInt(data, 2 * Bits.INT_SIZE_IN_BYTES, data.length - serializedByteArrayHeader.length, useBigEndian);
    }

    @CheckReturnValue
    private boolean putAsyncToMap(int partitionId, Supplier<Data> dataSupplier) {
        if (!Util.tryIncrement(numConcurrentAsyncOps, 1, JetService.MAX_PARALLEL_ASYNC_OPS)) {
            return false;
        }

        // we put a Data instance to the map directly to avoid the serialization of the byte array
        ICompletableFuture<Void> future = ((IMap) currentMap).setAsync(
                new SnapshotDataKey(partitionKeys[partitionId], partitionSequences[partitionId]++), dataSupplier.get());
        future.andThen(callback);
        numActiveFlushes.incrementAndGet();
        return true;
    }

    @Override
    @CheckReturnValue
    public boolean flush() {
        for (int i = 0; i < buffers.length; i++) {
            if (!flush(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean hasPendingAsyncOps() {
        return numActiveFlushes.get() > 0;
    }

    @Override
    public Throwable getError() {
        return lastError.getAndSet(null);
    }

    @Override
    public boolean isEmpty() {
        return numActiveFlushes.get() == 0 && Arrays.stream(buffers).allMatch(this::containsOnlyHeader);
    }

    int partitionKey(int partitionId) {
        return partitionKeys[partitionId];
    }

    public static final class SnapshotDataKey implements IdentifiedDataSerializable, PartitionAware {
        int partitionKey;
        int sequence;

        // for deserialization
        public SnapshotDataKey() {
        }

        SnapshotDataKey(int partitionKey, int sequence) {
            this.partitionKey = partitionKey;
            this.sequence = sequence;
        }

        @Override
        public Object getPartitionKey() {
            return partitionKey;
        }

        @Override
        public String toString() {
            return "SnapshotDataKey{partitionKey=" + partitionKey + ", sequence=" + sequence + '}';
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_KEY;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(partitionKey);
            out.writeInt(sequence);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            partitionKey = in.readInt();
            sequence = in.readInt();
        }
    }

    public static final class SnapshotDataValueTerminator implements IdentifiedDataSerializable {

        public static final IdentifiedDataSerializable INSTANCE = new SnapshotDataValueTerminator();

        private SnapshotDataValueTerminator() { }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_VALUE_TERMINATOR;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    /**
     * Variant of {@code java.io.ByteArrayOutputStream} with capacity limit.
     */
    static class CustomByteArrayOutputStream extends OutputStream {

        private static final byte[] EMPTY_BYTE_ARRAY = {};

        private byte[] data;
        private int size;
        private int capacityLimit;

        CustomByteArrayOutputStream(int capacityLimit) {
            this.capacityLimit = capacityLimit;
            // initial capacity is 0. It will take several reallocations to reach typical capacity,
            // but it's also common to remain at 0 - for partitions not assigned to us.
            data = EMPTY_BYTE_ARRAY;
        }

        @Override
        public void write(int b) {
            ensureCapacity(size + 1);
            data[size] = (byte) b;
            size++;
        }

        public void write(@Nonnull byte[] b, int off, int len) {
            if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
                throw new IndexOutOfBoundsException("off=" + off + ", len=" + len);
            }
            ensureCapacity(size + len);
            System.arraycopy(b, off, data, size, len);
            size += len;
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity - data.length > 0) {
                int newCapacity = data.length;
                do {
                    newCapacity = Math.max(1, newCapacity << 1);
                } while (newCapacity - minCapacity < 0);
                if (newCapacity - capacityLimit > 0) {
                    throw new IllegalStateException("buffer full");
                }
                data = Arrays.copyOf(data, newCapacity);
            }
        }

        void reset() {
            size = 0;
        }

        @Nonnull
        byte[] getInternalArray() {
            return data;
        }

        int size() {
            return size;
        }
    }
}
