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
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.kinesis.KinesisSinks;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KinesisSinkP<T> implements Processor {

    /**
     * Each shard can ingest a maximum of a 1000 records per second.
     */
    private static final int MAX_RECORD_PER_SHARD_PER_SECOND = 1000;

    /**
     * PutRecords requests are limited to 500 records.
     */
    private static final int MAX_RECORDS_IN_REQUEST = 500;

    /**
     * Since we are using PutRecords for its batching effect, we don't want
     * the batch size to be so small as to negate all benefits.
     */
    private static final int MIN_RECORDS_IN_REQUEST = 10;

    /**
     * The maximum allowed size of all the records in a PutRecords request,
     * including partition keys is 5M.
     */
    private static final int MAX_REQUEST_SIZE_IN_BYTES = 5 * 1024 * 1024;

    @Nonnull
    private final AmazonKinesisAsync kinesis;
    @Nonnull
    private final String stream;
    @Nullable
    private final ShardCountMonitor monitor;
    @Nonnull
    private final AtomicInteger shardCountProvider;
    @Nonnull
    private final Buffer<T> buffer;

    @Probe(name = "batchSize", unit = ProbeUnit.COUNT)
    private final Counter batchSize;
    @Probe(name = "throttlingSleep", unit = ProbeUnit.MS)
    private final Counter throttlingSleep = SwCounter.newSwCounter();

    private ILogger logger;
    private KinesisHelper helper;
    private int shardCount;
    private int sinkCount;

    private Future<PutRecordsResult> sendResult;
    private long nextSendTime = nanoTime();
    private final RetryTracker sendRetryTracker;

    private final ThroughputController throughputController = new ThroughputController();

    public KinesisSinkP(
            @Nonnull AmazonKinesisAsync kinesis,
            @Nonnull String stream,
            @Nonnull FunctionEx<T, String> keyFn,
            @Nonnull FunctionEx<T, byte[]> valueFn,
            @Nullable ShardCountMonitor monitor,
            @Nonnull AtomicInteger shardCountProvider,
            @Nonnull RetryStrategy retryStrategy
            ) {
        this.kinesis = kinesis;
        this.stream = stream;
        this.monitor = monitor;
        this.shardCountProvider = shardCountProvider;
        this.buffer = new Buffer<>(keyFn, valueFn);
        this.batchSize = SwCounter.newSwCounter(buffer.getCapacity());
        this.sendRetryTracker = new RetryTracker(retryStrategy);
    }

    @Override
    public boolean isCooperative() {
        return true;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        sinkCount = context.memberCount() * context.localParallelism();
        helper = new KinesisHelper(kinesis, stream);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true; //watermark ignored
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (monitor != null) {
            monitor.run();
        }

        updateThroughputLimitations();

        if (sendResult != null) {
            checkIfSendingFinished();
        }
        if (sendResult == null) {
            initSending(inbox);
        }
    }

    @Override
    public boolean complete() {
        if (sendResult != null) {
            checkIfSendingFinished();
        }
        if (sendResult == null) {
            if (buffer.isEmpty()) {
                return true;
            }
            initSending(null);
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (sendResult != null) {
            checkIfSendingFinished();
        }
        return sendResult == null;
    }

    private void updateThroughputLimitations() {
        int newShardCount = shardCountProvider.get();
        if (newShardCount > 0 && shardCount != newShardCount) {
            buffer.setCapacity(throughputController.computeBatchSize(newShardCount, sinkCount));
            batchSize.set(buffer.getCapacity());

            shardCount = newShardCount;
        }
    }

    private void initSending(@Nullable Inbox inbox) {
        if (inbox != null) {
            bufferFromInbox(inbox);
        }
        attemptToDispatchBufferContent();
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

    private void attemptToDispatchBufferContent() {
        if (buffer.isEmpty()) {
            return;
        }

        long currentTime = nanoTime();
        if (currentTime < nextSendTime) {
            return;
        }

        List<PutRecordsRequestEntry> entries = buffer.content();
        sendResult = helper.putRecordsAsync(entries);
        nextSendTime = currentTime;
    }

    private void checkIfSendingFinished() {
        if (sendResult.isDone()) {
            PutRecordsResult result;
            try {
                result = helper.readResult(this.sendResult);
            } catch (ProvisionedThroughputExceededException pte) {
                dealWithThroughputExceeded("Data throughput rate exceeded. Backing off and retrying in %d ms");
                return;
            } catch (SdkClientException sce) {
                dealWithSendFailure(sce);
                return;
            } catch (Throwable t) {
                throw rethrow(t);
            } finally {
                sendResult = null;
            }

            pruneSentFromBuffer(result);
            if (result.getFailedRecordCount() > 0) {
                dealWithThroughputExceeded("Failed to send " + result.getFailedRecordCount() + " (out of " +
                        result.getRecords().size() + ") record(s) to stream '" + stream +
                        "'. Sending will be retried in %d ms, message reordering is likely.");
            } else {
                long sleepTimeNanos = throughputController.markSuccessfulSend();
                this.nextSendTime += sleepTimeNanos;
                this.throttlingSleep.set(NANOSECONDS.toMillis(sleepTimeNanos));
                sendRetryTracker.reset();
            }
        }
    }

    private void dealWithSendFailure(@Nonnull Exception failure) {
        sendRetryTracker.attemptFailed();
        if (sendRetryTracker.shouldTryAgain()) {
            long timeoutMillis = sendRetryTracker.getNextWaitTimeMillis();
            logger.warning(String.format("Failed to send records, will retry in %d ms. Cause: %s",
                    timeoutMillis, failure.getMessage()));
            nextSendTime = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
        } else {
            throw rethrow(failure);
        }

    }

    private void dealWithThroughputExceeded(@Nonnull String message) {
        long sleepTimeNanos = throughputController.markFailedSend();
        this.nextSendTime += sleepTimeNanos;
        this.throttlingSleep.set(NANOSECONDS.toMillis(sleepTimeNanos));
        logger.warning(String.format(message, NANOSECONDS.toMillis(sleepTimeNanos)));
    }

    private void pruneSentFromBuffer(@Nullable PutRecordsResult result) {
        if (result == null) {
            return;
        }

        List<PutRecordsResultEntry> resultEntries = result.getRecords();
        if (result.getFailedRecordCount() > 0) {
            for (int i = resultEntries.size() - 1; i >= 0; i--) {
                PutRecordsResultEntry resultEntry = resultEntries.get(i);
                if (resultEntry.getErrorCode() == null) {
                    buffer.remove(i);
                }
            }
        } else {
            buffer.remove(0, resultEntries.size());
        }
    }

    private static class Buffer<T> {

        private final FunctionEx<T, String> keyFn;
        private final FunctionEx<T, byte[]> valueFn;

        private final BufferEntry[] entries;
        private int entryCount;
        private int totalEntrySize;
        private int capacity;

        Buffer(FunctionEx<T, String> keyFn, FunctionEx<T, byte[]> valueFn) {
            this.keyFn = keyFn;
            this.valueFn = valueFn;
            this.entries = initEntries();
            this.capacity = entries.length;
        }

        public int getCapacity() {
            return capacity;
        }

        void setCapacity(int capacity) {
            if (capacity < 0 || capacity > entries.length) {
                throw new IllegalArgumentException("Capacity limited to [0, " + entries.length + ")");
            }
            this.capacity = capacity;
        }

        boolean add(T item) {
            if (isFull()) {
                return false;
            }

            String key = keyFn.apply(item);
            if (key.isEmpty()) {
                throw new JetException("Key empty");
            }
            int unicodeCharsInKey = key.length();
            if (unicodeCharsInKey > KinesisSinks.MAXIMUM_KEY_LENGTH) {
                throw new JetException("Key too long");
            }
            int keyLength = getKeyLength(key);

            byte[] value = valueFn.apply(item);
            int itemLength = value.length + keyLength;
            if (itemLength > KinesisSinks.MAX_RECORD_SIZE) {
                throw new JetException("Encoded length (key + payload) is too big");
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

        public void remove(int index) {
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

        public void remove(int index, int count) {
            if (count == 0) {
                return;
            }
            if (count < 0) {
                throw new IllegalArgumentException("Count has to be non-negative");
            }

            if (index < 0 || index >= entryCount) {
                throw new IllegalArgumentException("Index needs to be in [0, " + entryCount + ")");
            }

            if (index == 0 && count == entryCount) {
                clear();
            } else {
                for (int i = 0; i < count; i++) {
                    remove(index);
                }
            }
        }

        void clear() {
            entryCount = 0;
            totalEntrySize = 0;
        }

        boolean isEmpty() {
            return entryCount == 0;
        }

        public boolean isFull() {
            return entryCount == entries.length || entryCount >= capacity;
        }

        public List<PutRecordsRequestEntry> content() {
            return Arrays.stream(entries)
                    .limit(Math.min(entryCount, capacity))
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

    /**
     * Under normal circumstances, when our sinks don't saturate the stream
     * (ie. they don't send out more data than the sink can ingest), sinks
     * will not sleep or introduce any kind of delay between two consecutive
     * send operations (as long as there is data to send).
     * <p>
     * When the stream's ingestion rate is reached however, we want the sinks to
     * slow down the sending process. They do it by adjusting the send batch
     * size on one hand, and by introducing a sleep after each send operation.
     * <p>
     * The sleep duration is continuously adjusted to find the best value. The
     * ideal situation we try to achieve is that we never trip the stream's
     * ingestion rate limiter, while also sending out data with the maximum
     * rate possible.
     */
    private static final class ThroughputController {

        /**
         * The ideal sleep duratio after sends, while rate limiting is necessary.
         */
        private static final int IDEAL_SLEEP_MS = 250;

        /**
         * Minimum sleep duration after sends, while rate limiting is necessary.
         */
        private static final long MINIMUM_SLEEP_MS = 100L;

        /**
         * Maximum sleep duration after sends, while rate limiting is necessary.
         */
        private static final long MAXIMUM_SLEEP_MS = 10_000L;

        /**
         * The increment of increasing/decreasing the sleep duration while
         * adaptively searching for the best value.
         */
        private static final int SLEEP_INCREMENT = 25;

        /**
         * When adjusting the sleep duration we want to handle the effects in a
         * non-equal manner. If we set a sleep duration and then still get
         * messages rejected by the stream, we immediately increase the
         * duration.
         * <p>
         * On successful sends however we don't necessarily want to react
         * immediately. A success means that the sleep duration we have used is
         * good, so we should keep it and use it for following sends.
         * <p>
         * Keeping the last used sleep duration for ever is also not a good idea,
         * because the data flow might have decreased in the meantime and we
         * might be sleeping too much. So we want to slowly decrease, just
         * not as fast as we have increased it.
         * <p>
         * The relative ratio of these two speeds is what this constant
         * expresses.
         */
        private static final int DEGRADATION_VS_RECOVERY_SPEED_RATIO = 10;

        private final Damper<Long> damper;

        ThroughputController() {
            this.damper = initDamper();
        }

        int computeBatchSize(int shardCount, int sinkCount) {
            if (shardCount < 1) {
                throw new IllegalArgumentException("Invalid shard count: " + shardCount);
            }
            if (sinkCount < 1) {
                throw new IllegalArgumentException("Invalid sink count: " + sinkCount);
            }

            int totalRecordsPerSecond = MAX_RECORD_PER_SHARD_PER_SECOND * shardCount;
            int recordPerSinkPerSecond = totalRecordsPerSecond / sinkCount;
            int computedBatchSize = recordPerSinkPerSecond / (int) (SECONDS.toMillis(1) / IDEAL_SLEEP_MS);

            if (computedBatchSize > MAX_RECORDS_IN_REQUEST) {
                return MAX_RECORDS_IN_REQUEST;
            } else {
                return Math.max(computedBatchSize, MIN_RECORDS_IN_REQUEST);
            }
        }

        long markSuccessfulSend() {
            damper.ok();
            return damper.output();
        }

        long markFailedSend() {
            damper.error();
            return damper.output();
        }

        private static Damper<Long> initDamper() {
            List<Long> list = new ArrayList<>();

            //default sleep when there haven't been errors for a while
            list.add(0L);

            //increasing sleeps when errors keep repeating
            for (long millis = MINIMUM_SLEEP_MS; millis <= MAXIMUM_SLEEP_MS; millis += SLEEP_INCREMENT) {
                list.add(MILLISECONDS.toNanos(millis));
            }

            return new Damper<>(DEGRADATION_VS_RECOVERY_SPEED_RATIO, list.toArray(new Long[0]));
        }
    }
}
