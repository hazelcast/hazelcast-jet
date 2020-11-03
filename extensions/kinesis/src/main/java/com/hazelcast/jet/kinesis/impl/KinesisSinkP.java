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
import java.util.ArrayList;
import java.util.List;

public class KinesisSinkP<T> implements Processor {

    /**
     * The number of Unicode characters making up keys is limited to a maximum
     * of 256.
     */
    private static final int MAX_UNICODE_CHARS_IN_KEY = 256;

    @Nonnull
    private final AmazonKinesisAsync kinesis;
    @Nonnull
    private final String stream;
    @Nonnull
    FunctionEx<T, String> keyFn;
    @Nonnull
    FunctionEx<T, byte[]> valueFn;

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
        } else {
            buffer.clear();
        }
    }

    private boolean isStreamActive() {
        StreamDescription description = kinesis.describeStream(stream).getStreamDescription();
        return "ACTIVE".equals(description.getStreamStatus());
    }

    private static class Buffer<T> {

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

        private final FunctionEx<T, String> keyFn;
        private final FunctionEx<T, byte[]> valueFn;

        private final List<PutRecordsRequestEntry> entries = new ArrayList<>(MAX_RECORDS_IN_REQUEST);

        //todo: could reuse PutRecordsRequestEntry

        private int totalEntrySize;

        public Buffer(FunctionEx<T, String> keyFn, FunctionEx<T, byte[]> valueFn) {
            this.keyFn = keyFn;
            this.valueFn = valueFn;
        }

        boolean add(T item) {
            String key = keyFn.apply(item);
            int unicodeCharsInKey = key.length();
            if (unicodeCharsInKey > MAX_UNICODE_CHARS_IN_KEY) {
                throw new IllegalArgumentException("Key of " + item + " too long");
            }
            int keyLength = getKeyLength(key);

            byte[] value = valueFn.apply(item);
            int itemLenght = value.length + keyLength;
            if (itemLenght > MAX_RECORD_SIZE_IN_BYTES) {
                throw new IllegalArgumentException("Item " + item + " encoded length (key + payload) is too big");
            }

            if (totalEntrySize + itemLenght > MAX_REQUEST_SIZE_IN_BYTES) {
                return false;
            } else {
                totalEntrySize += itemLenght;

                PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
                entry.setPartitionKey(key);
                entry.setData(ByteBuffer.wrap(value));

                entries.add(entry);

                return true;
            }
        }

        public void remove(int index) {
            PutRecordsRequestEntry entry = entries.remove(index);
            int keyLength = getKeyLength(entry.getPartitionKey());
            ByteBuffer bb = entry.getData();
            int valueLength = bb.limit();
            totalEntrySize -= keyLength + valueLength;
        }

        boolean isEmpty() {
            return totalEntrySize == 0;
        }

        void clear() {
            entries.clear();
            totalEntrySize = 0;
        }

        public List<PutRecordsRequestEntry> content() {
            return entries;
        }

        private int getKeyLength(String key) {
            return key.getBytes(StandardCharsets.UTF_8).length; //todo: does AWS actually use UTF-8?
        }
    }
}
