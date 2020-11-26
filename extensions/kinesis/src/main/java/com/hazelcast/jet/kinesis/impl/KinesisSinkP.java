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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
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
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KinesisSinkP<T> implements Processor {

    //todo; Each shard can support writes up to 1,000 records per second, up to a
    // maximum data write total of 1 MiB per second. Right now we don't pre-check
    // this, because we don't check which item goes to what shard, we just rely
    // failure handling with exponential backoff. Would be complicated to improve
    // on, but can we affort not to?

    /**
     * PutRecords requests are limited to 500 records.
     */
    private static final int MAX_RECORDS_IN_REQUEST = 500;

    private static final long PAUSE_AFTER_FAILURE = SECONDS.toNanos(1); //todo: exponential backoff

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

    private final Buffer<T> buffer;

    private ILogger logger;
    private KinesisHelper helper;

    private State state = State.READY_TO_SEND;
    private long nextSendTime = System.nanoTime();
    private Future<PutRecordsResult> sendResult;

    public KinesisSinkP(
            AmazonKinesisAsync kinesis,
            @Nonnull String stream,
            @Nonnull FunctionEx<T, String> keyFn,
            @Nonnull FunctionEx<T, byte[]> valueFn
    ) {
        this.kinesis = kinesis;
        this.stream = stream;
        this.buffer = new Buffer<>(keyFn, valueFn);
    }

    @Override
    public boolean isCooperative() {
        return true;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        helper = new KinesisHelper(kinesis, stream, logger);
        helper.waitForStreamToActivate();
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true; //watermark ignored
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (state == State.SENDING_IN_PROGRESS) {
            handleSendingInProgress();
        }
        if (state == State.READY_TO_SEND) {
            handleReadyToSend(inbox);
        }
    }

    @Override
    public boolean complete() {
        if (state == State.SENDING_IN_PROGRESS) {
            handleSendingInProgress();
        }
        if (state == State.READY_TO_SEND) {
            if (buffer.isEmpty()) {
                return true;
            }
            handleReadyToSend(null);
        }
        return false;
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

    private void handleReadyToSend(@Nullable Inbox inbox) {
        if (inbox != null) {
            bufferFromInbox(inbox);
        }
        if (attemptToDispatchBufferContent()) {
            state = State.SENDING_IN_PROGRESS;
        }
    }

    private boolean attemptToDispatchBufferContent() {
        if (buffer.isEmpty() || System.nanoTime() < nextSendTime) {
            return false;
        }

        List<PutRecordsRequestEntry> entries = buffer.content();
        sendResult = helper.putRecordsAsync(entries);

        nextSendTime = System.nanoTime(); //todo: add some wait here?
        return true;
    }

    private void handleSendingInProgress() {
        if (sendResult.isDone()) {
            try {
                PutRecordsResult result = helper.readResult(this.sendResult);
                pruneSentFromBuffer(result);
                if (result.getFailedRecordCount() > 0) {
                    dealWithSendFailure("Sending only partially successful. Retry sending failed items (ordering" +
                            " will be affected). ");
                } else {
                    dealWithSendSuccessful();
                }
            } catch (ProvisionedThroughputExceededException pte) {
                dealWithSendFailure("Data throughput rate exceeded. Backing off and retrying.");
            } catch (SdkClientException sce) {
                dealWithSendFailure("Failed reading records, retrying. Cause: " + sce.getMessage());
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
    }

    private void dealWithSendSuccessful() {
        state = State.READY_TO_SEND;
    }

    private void dealWithSendFailure(@Nonnull String message) {
        logger.warning(message);
        nextSendTime = System.nanoTime() + PAUSE_AFTER_FAILURE;
        state = State.READY_TO_SEND;
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
        } else {
            buffer.clear();
        }
    }

    private enum State {
        /**
         * Ready to send data to Kinesis, if available.
         */
        READY_TO_SEND,

        /**
         * Data has been sent to Kinesis, waiting for a reply.
         */
        SENDING_IN_PROGRESS,
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
            entryCount--;
            if (index < entryCount) {
                BufferEntry tmp = entries[index];
                System.arraycopy(entries, index + 1, entries, index, entryCount - index);
                entries[entryCount] = tmp;
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

        private PutRecordsRequestEntry putRecordsRequestEntry;
        private int encodedSize;

        public void set(String partitionKey, byte[] data, int size) {
            if (putRecordsRequestEntry == null) {
                putRecordsRequestEntry = new PutRecordsRequestEntry();
            }

            putRecordsRequestEntry.setPartitionKey(partitionKey);

            ByteBuffer byteBuffer = putRecordsRequestEntry.getData();
            if (byteBuffer == null || byteBuffer.capacity() < data.length) {
                putRecordsRequestEntry.setData(ByteBuffer.wrap(data));
            } else {
                byteBuffer.clear();
                byteBuffer.put(data);
                byteBuffer.flip();
            }

            encodedSize = size;
        }
    }

}