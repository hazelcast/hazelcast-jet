/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KinesisSinkP<T> implements Processor {

    /**
     * PutRecords requests are limited to 500 records.
     */
    private static final int MAX_RECORDS_IN_REQUEST = 500;


    /**
     * Each record, when encoded as a byte array, is limited to 1M,
     * including the partition key (Unicode String).
     */
    private static final int MAX_RECORD_SIZE_IN_BYTES = 1024 * 1024;

    /**
     * The maximum allowed size of all the records in a PutRecords request,
     * including partition keys is 5M.
     */
    private static final int MAX_REQUEST_SIZE_IN_BYTES = 5 * 1024 * 1024;

    /**
     * The number of Unicode characters making up keys is limited to a maximum
     * of 256.
     */
    private static final int MAX_UNICODE_CHARS_IN_KEY = 256;

    private final AmazonKinesisAsync kinesis;
    private final String stream;
    private final FunctionEx<T, String> keyFn;
    private final FunctionEx<T, byte[]> valueFn;

    private final Buffer<T> buffer;

    private ILogger logger;

    public KinesisSinkP(
            AmazonKinesisAsync kinesis,
            @Nonnull String stream,
            @Nonnull FunctionEx<T, String> keyFn,
            @Nonnull FunctionEx<T, byte[]> valueFn
    ) {
        this.kinesis = kinesis;
        this.stream = stream;
        this.keyFn = keyFn;
        this.valueFn = valueFn;
        this.buffer = new Buffer<>(keyFn, valueFn);
    }

    @Override
    public boolean isCooperative() {
        return false; //todo: can be made cooperative
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();

        while (!isStreamActive()) {
            logger.info("Waiting for stream  + " + stream + " to become active...");
        }
    }

    @Override
    public boolean tryProcess() {
        return isStreamActive();
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true; //watermark ignored
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!isStreamActive()) {
            return;
        }

        bufferFromInbox(inbox);
        PutRecordsResult result = dispatchBufferContent();
        pruneSentFromBuffer(result);
    }

    private void bufferFromInbox(@Nonnull Inbox inbox) {
        if (buffer.isFull()) {
            return;
        }

        while (true) {
            T t = (T) inbox.peek();
            if (t == null) {
                //no more items in inbox
                return;
            }

            boolean canBeBuffered = buffer.add(t);
            if (canBeBuffered) {
                inbox.remove();
            } else {
                //no more room in buffer
                return;
            }
        }
    }

    @Nullable
    private PutRecordsResult dispatchBufferContent() {
        if (!buffer.isEmpty()) {

            PutRecordsRequest request = new PutRecordsRequest();
            request.setRecords(buffer.content());
            request.setStreamName(stream);

            return kinesis.putRecords(request);
        }
        return null;
    }

    private void pruneSentFromBuffer(@Nullable PutRecordsResult result) {
        if (result == null) {
            return;
        }

        int failCount = result.getFailedRecordCount();
        if (failCount > 0) {
            logger.warning("Failed sending " + failCount + " records to stream " + stream + ". " +
                    "Sending them will be retried, but reordering might be unavoidable.");
            List<PutRecordsResultEntry> resultEntries = result.getRecords();
            for (int i = resultEntries.size() - 1; i >= 0; i--) {
                PutRecordsResultEntry resultEntry = resultEntries.get(i);
                if (resultEntry.getErrorCode() == null) {
                    buffer.remove(i);
                }
            }
            //todo: need exponential backoff, before retrying
        } else {
            buffer.clear();
        }
    }

    private boolean isStreamActive() {
        StreamDescription description = kinesis.describeStream(stream).getStreamDescription();
        return "ACTIVE".equals(description.getStreamStatus());
    }

    private static class Buffer<T> {

        private final FunctionEx<T, String> keyFn;
        private final FunctionEx<T, byte[]> valueFn;

        private final BufferEntry[] entries;
        private int entryCount;
        private int totalEntrySize;

        Buffer(FunctionEx<T, String> keyFn, FunctionEx<T, byte[]> valueFn) {
            this.keyFn = keyFn;
            this.valueFn = valueFn;
            this.entries = initEntries();
        }

        boolean add(T item) {
            if (entryCount == entries.length) {
                return false;
            }

            String key = keyFn.apply(item);
            int unicodeCharsInKey = key.length();
            if (unicodeCharsInKey > MAX_UNICODE_CHARS_IN_KEY) {
                throw new IllegalArgumentException("Key of " + item + " too long");
            }
            int keyLength = getKeyLength(key);

            byte[] value = valueFn.apply(item);
            int itemLength = value.length + keyLength;
            if (itemLength > MAX_RECORD_SIZE_IN_BYTES) {
                throw new IllegalArgumentException("Item " + item + " encoded length (key + payload) is too big");
            }

            if (totalEntrySize + itemLength > MAX_REQUEST_SIZE_IN_BYTES) {
                return false;
            } else {
                totalEntrySize += itemLength;

                BufferEntry entry = entries[entryCount++];
                entry.set(key, value, itemLength);

                return true;
            }
        }

        public void remove(int index) { //todo: test it, at least manually
            if (index < 0 || index >= entryCount) {
                throw new IllegalArgumentException("Index needs to be between 0 and " + entryCount);
            }

            totalEntrySize -= entries[index].encodedSize;

            int lastIndex = entryCount - 1;
            if (index < lastIndex) {
                BufferEntry tmp = entries[index];
                entries[index] = entries[lastIndex];
                entries[lastIndex] = tmp;
            } else {
                entryCount--;
            }
        }

        boolean isEmpty() {
            return entryCount == 0;
        }

        public boolean isFull() {
            return entryCount == entries.length;
        }

        void clear() {
            entryCount = 0;
            totalEntrySize = 0;
        }

        public List<PutRecordsRequestEntry> content() {
            return Arrays.stream(entries)
                    .limit(entryCount)
                    .map(e -> e.putRecordsRequestEntry)
                    .collect(Collectors.toList());
        }

        private int getKeyLength(String key) {
            return key.getBytes(StandardCharsets.UTF_8).length; //todo: does AWS actually use UTF-8?
        }

        private static BufferEntry[] initEntries() {
            return IntStream.range(0, MAX_RECORDS_IN_REQUEST).boxed()
                    .map(IGNORED -> new BufferEntry())
                    .toArray(BufferEntry[]::new);
        }
    }

    private static final class BufferEntry {

        private final PutRecordsRequestEntry putRecordsRequestEntry;

        private int encodedSize;

        BufferEntry() {
            putRecordsRequestEntry = new PutRecordsRequestEntry();
            encodedSize = 0;
        }

        public void set(String partitionKey, byte[] data, int size) {
            putRecordsRequestEntry.setPartitionKey(partitionKey);

            ByteBuffer byteBuffer = putRecordsRequestEntry.getData();
            if (byteBuffer == null || byteBuffer.capacity() < data.length) {
                putRecordsRequestEntry.setData(ByteBuffer.wrap(data));
            } else {
                byteBuffer.clear();
                byteBuffer.put(data);
                byteBuffer.flip();
            }

            //todo: do we ever want to shrink the buffers?

            encodedSize = size;
        }
    }
}
