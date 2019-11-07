/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.processor.TwoTransactionProcessorUtility;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.logging.ILogger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * See {@link KafkaProcessors#writeKafkaP}.
 */
public final class WriteKafkaP<T, K, V> implements Processor {

    private final Map<String, Object> properties;
    private final Function<? super T, ? extends ProducerRecord<K, V>> toRecordFn;
    private final boolean exactlyOnce;

    private Context context;
    private TwoTransactionProcessorUtility<KafkaTransactionId, KafkaTransaction<K, V>> snapshotUtility;
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();

    private final Callback callback = (metadata, exception) -> {
        // Note: this method may be called on different thread.
        if (exception != null) {
            lastError.compareAndSet(null, exception);
        }
    };

    private WriteKafkaP(
            @Nonnull Map<String, Object> properties,
            @Nonnull Function<? super T, ? extends ProducerRecord<K, V>> toRecordFn,
            boolean exactlyOnce
    ) {
        this.properties = properties;
        this.toRecordFn = toRecordFn;
        this.exactlyOnce = exactlyOnce;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        this.context = context;
        snapshotUtility = new TwoTransactionProcessorUtility<>(
                outbox,
                context,
                context.processingGuarantee() == EXACTLY_ONCE && !exactlyOnce
                        ? AT_LEAST_ONCE
                        : context.processingGuarantee(),
                (processorIndex, txnIndex) -> new KafkaTransactionId(
                        context.jobId(), context.jobConfig().getName(), context.vertexName(), processorIndex, txnIndex),
                txnId -> {
                    if (txnId != null) {
                        properties.put("transactional.id", txnId.getKafkaId());
                    }
                    return new KafkaTransaction<>(txnId, properties, context.logger());
                },
                txnId -> {
                    try {
                        recoverTransaction(txnId, true);
                    } catch (ProducerFencedException e) {
                        context.logger().warning("Failed to finish the commit of a transaction ID saved in the " +
                                "snapshot, data loss can occur. Transaction id: " + txnId.getKafkaId(), e);
                    }
                },
                txnId -> recoverTransaction(txnId, false)
        );
    }

    @Override
    public boolean tryProcess() {
        checkError();
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        KafkaTransaction<K, V> txn = snapshotUtility.activeTransaction();
        if (txn == null) {
            context.logger().finest("no transaction");
            return;
        }
        checkError();
        for (Object item; (item = inbox.peek()) != null; ) {
            try {
                txn.producer.send(toRecordFn.apply((T) item), callback);
                context.logger().finest("sent " + item); // TODO [viliam] remove
            } catch (TimeoutException ignored) {
                // apply backpressure, the item will be retried
                return;
            }
            inbox.remove();
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public boolean complete() {
        KafkaTransaction<K, V> transaction = snapshotUtility.activeTransaction();
        if (transaction == null) {
            return false;
        }
        transaction.producer.flush();
        LoggingUtil.logFinest(context.logger(), "flush in complete() done, %s", transaction.transactionId);
        checkError();
        snapshotUtility.afterCompleted();
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        if (!snapshotUtility.saveToSnapshot()) {
            return false;
        }
        checkError();
        return true;
    }

    @Override
    public boolean onSnapshotCompleted(boolean commitTransactions) {
        return snapshotUtility.onSnapshotCompleted(commitTransactions);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        snapshotUtility.restoreFromSnapshot(inbox);
    }

    @Override
    public boolean finishSnapshotRestore() {
        return snapshotUtility.finishSnapshotRestore();
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private void recoverTransaction(KafkaTransactionId txnId, boolean commit) {
        context.logger().fine("recoverTransaction " + txnId + ", commit=" + commit);
        HashMap<Object, Object> properties2 = new HashMap<>(properties);
        properties2.put("transactional.id", txnId.getKafkaId());
        try (KafkaProducer p  = new KafkaProducer(properties2)) {
            if (commit) {
                ResumeTransactionUtil.resumeTransaction(context.logger(), p, txnId.producerId(), txnId.epoch(),
                        txnId.getKafkaId());
                p.commitTransaction();
            } else {
                p.initTransactions();
            }
        }
    }

    @Override
    public void close() {
        if (snapshotUtility != null) {
            snapshotUtility.close();
        }
    }

    private void checkError() {
        Throwable t = lastError.get();
        if (t != null) {
            throw sneakyThrow(t);
        }
    }

    /**
     * Use {@link KafkaProcessors#writeKafkaP(Properties, FunctionEx, boolean)}
     */
    @SuppressWarnings("unchecked")
    public static <T, K, V> SupplierEx<Processor> supplier(
            @Nonnull Properties properties,
            @Nonnull Function<? super T, ? extends ProducerRecord<K, V>> toRecordFn,
            boolean exactlyOnce
    ) {
        if (properties.containsKey("transactional.id")) {
            throw new IllegalArgumentException("Property `transactional.id` must not be set, Jet sets it as needed");
        }
        return () -> new WriteKafkaP<>(new HashMap<>((Map) properties), toRecordFn, exactlyOnce);
    }

    /**
     * A simple class wrapping a KafkaProducer that ensures that
     * `producer.initTransactions` is called exactly once before first
     * `beginTransaction` call.
     */
    private static final class KafkaTransaction<K, V> implements TransactionalResource<KafkaTransactionId> {
        private final KafkaProducer<K, V> producer;
        private final ILogger logger;
        private final KafkaTransactionId transactionId;
        private boolean txnInitialized;

        private KafkaTransaction(KafkaTransactionId transactionId, Map<String, Object> properties, ILogger logger) {
            this.transactionId = transactionId;
            this.producer = new KafkaProducer<>(properties);
            this.logger = logger;
        }

        @Override
        public KafkaTransactionId id() {
            return transactionId;
        }

        @Override
        public void beginTransaction() {
            if (!txnInitialized) {
                LoggingUtil.logFinest(logger, "initTransactions %s", transactionId);
                txnInitialized = true;
                producer.initTransactions();
                transactionId.updateProducerAndEpoch(producer);
            }
            LoggingUtil.logFinest(logger, "beginTransaction %s", transactionId);
            producer.beginTransaction();
        }

        @Override
        public void prepare() {
            LoggingUtil.logFinest(logger, "prepare %s", transactionId);
            producer.flush();
        }

        @Override
        public void flush() {
            LoggingUtil.logFinest(logger, "flush %s", transactionId);
            producer.flush();
        }

        @Override
        public void commit() {
            LoggingUtil.logFinest(logger, "commitTransaction %s", transactionId);
            producer.commitTransaction();
        }

        @Override
        public void release() {
            LoggingUtil.logFinest(logger, "release (close producer) %s", transactionId);
            producer.close();
        }
    }

    // TODO [viliam] better serialization
    public static class KafkaTransactionId implements TwoPhaseSnapshotCommitUtility.TransactionId, Serializable {
        private final long jobId;
        private final String jobName;
        private final String vertexId;
        private final int processorIndex;
        private final int transactionIndex;
        private long producerId = -1;
        private short epoch = -1;

        KafkaTransactionId(long jobId, String jobName, @Nonnull String vertexId, int processorIndex,
                           int transactionIndex) {
            this.jobId = jobId;
            this.jobName = jobName;
            this.vertexId = vertexId;
            this.processorIndex = processorIndex;
            this.transactionIndex = transactionIndex;
        }

        @Override
        public int index() {
            return processorIndex;
        }

        long producerId() {
            return producerId;
        }

        short epoch() {
            return epoch;
        }

        void updateProducerAndEpoch(KafkaProducer producer) {
            producerId = ResumeTransactionUtil.getProducerId(producer);
            epoch = ResumeTransactionUtil.getEpoch(producer);
        }

        /**
         * Get a String representation of this ID.
         */
        @Override
        public String toString() {
            return getKafkaId() + ",producerId=" + producerId + ",epoch=" + epoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KafkaTransactionId that = (KafkaTransactionId) o;
            return this.getKafkaId().equals(that.getKafkaId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, vertexId, processorIndex);
        }

        @Nonnull
        String getKafkaId() {
            return "jet.job-" + idToString(jobId) + '.' + sanitize(jobName) + '.' + sanitize(vertexId) + '.'
                    + processorIndex + "-" + transactionIndex;
        }

        private static String sanitize(String s) {
            if (s == null) {
                return "null";
            }
            return s.replaceAll("[^\\p{Alnum}.\\-_$#/{}\\[\\]]", "_");
        }
    }
}
