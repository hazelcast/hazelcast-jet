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

package com.hazelcast.jet.kafka;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.kafka.KafkaProcessors.writeKafkaP;

/**
 * Contains factory methods for Apache Kafka sinks.
 *
 * @since 3.0
 */
public final class KafkaSinks {

    private KafkaSinks() {
    }

    /**
     * Returns a source that publishes messages to an Apache Kafka topic.
     * It transforms each received item to a {@code ProducerRecord} using the
     * supplied mapping function.
     * <p>
     * The source creates a single {@code KafkaProducer} per cluster
     * member using the supplied {@code properties}.
     * <p>
     * Behavior on job restart: the processor is stateless. On snapshot we only
     * make sure that all async operations are done. If the job is restarted,
     * duplicate events can occur. If you need exactly-once behavior, you must
     * ensure idempotence on the application level.
     * <p>
     * IO failures are generally handled by Kafka producer and do not cause the
     * processor to fail. Refer to Kafka documentation for details.
     * <p>
     * Default local parallelism for this processor is 2 (or less if less CPUs
     * are available).
     *
     * TODO [viliam] document exactly-once behavior. Document also:
     *  - the txn timeout and possible loss of data
     *  - the overhead of transactions, greater latency
     *
     * @param properties     producer properties which should contain broker
     *                       address and key/value serializers
     * @param toRecordFn   function that extracts the key from the stream item
     *
     * @param <E> type of stream item
     * @param <K> type of the key published to Kafka
     * @param <V> type of the value published to Kafka
     */
    @Nonnull
    public static <E, K, V> Sink<E> kafka(
            @Nonnull Properties properties,
            @Nonnull FunctionEx<? super E, ProducerRecord<K, V>> toRecordFn
    ) {
        // TODO [viliam] default for exactlyOnce?
        return Sinks.fromProcessor("writeKafka", writeKafkaP(properties, toRecordFn, true));
    }

    /**
     * Convenience for {@link #kafka(Properties, FunctionEx)} which creates
     * a {@code ProducerRecord} using the given topic and the given key and value
     * mapping functions
     *
     * @param <E> type of stream item
     * @param <K> type of the key published to Kafka
     * @param <V> type of the value published to Kafka
     * @param properties     producer properties which should contain broker
     *                       address and key/value serializers
     * @param topic          name of the Kafka topic to publish to
     * @param extractKeyFn   function that extracts the key from the stream item
     * @param extractValueFn function that extracts the value from the stream item
     */
    @Nonnull
    public static <E, K, V> Sink<E> kafka(
            @Nonnull Properties properties,
            @Nonnull String topic,
            @Nonnull FunctionEx<? super E, K> extractKeyFn,
            @Nonnull FunctionEx<? super E, V> extractValueFn
    ) {
        return Sinks.fromProcessor("writeKafka(" + topic + ")",
                writeKafkaP(properties, topic, extractKeyFn, extractValueFn, true));
    }

    /**
     * Convenience for {@link #kafka(Properties, String, FunctionEx, FunctionEx)}
     * which expects {@code Map.Entry<K, V>} as input and extracts its key and value
     * parts to be published to Kafka.
     *
     * @param <K>        type of the key published to Kafka
     * @param <V>        type of the value published to Kafka
     * @param properties producer properties which should contain broker
     *                   address and key/value serializers
     * @param topic      Kafka topic name to publish to
     */
    @Nonnull
    public static <K, V> Sink<Entry<K, V>> kafka(@Nonnull Properties properties, @Nonnull String topic) {
        return kafka(properties, topic, Entry::getKey, Entry::getValue);
    }

    /**
     * TODO [viliam]
     * @param properties
     * @param <E>
     * @return
     */
    @Nonnull
    public static <E> Builder<E> kafka(@Nonnull Properties properties) {
        return new Builder<>(properties);
    }

    /**
     * TODO [viliam]
     * @param <E>
     */
    public static final class Builder<E> {

        private final Properties properties;
        private FunctionEx<? super E, ? extends ProducerRecord<Object, Object>> toRecordFn;
        private String topic;
        private FunctionEx<? super E, ?> extractKeyFn;
        private FunctionEx<? super E, ?> extractValueFn;
        private boolean exactlyOnce = true;

        private Builder(Properties properties) {
            this.properties = properties;
        }

        /**
         * TODO [viliam]
         * @param toRecordFn a function to convert sunk items into Kafka's
         *      {@code ProducerRecord}
         * @return this instance for fluent API
         */
        @SuppressWarnings("unchecked")
        public Builder<E> toRecordFn(FunctionEx<? super E, ? extends ProducerRecord<?, ?>> toRecordFn) {
            if (topic != null || extractKeyFn != null || extractValueFn != null) {
                throw new IllegalArgumentException("topic, extractKeyFn or extractValueFn are already set, you can't use" +
                        " toRecordFn along with them");
            }
            this.toRecordFn = (FunctionEx<? super E, ? extends ProducerRecord<Object, Object>>) toRecordFn;
            return this;
        }

        /**
         * TODO [viliam]
         * @param topic the topic name
         * @return this instance for fluent API
         */
        public Builder<E> topic(String topic) {
            if (toRecordFn != null) {
                throw new IllegalArgumentException("toRecordFn already set, you can't use topic if it's set");
            }
            this.topic = topic;
            return this;
        }

        /**
         * TODO [viliam]
         * The default is to use {@code null} key.
         *
         * @param extractKeyFn a function to extract the key from the sunk item
         * @return this instance for fluent API
         */
        public Builder<E> extractKeyFn(@Nonnull FunctionEx<? super E, ?> extractKeyFn) {
            if (toRecordFn != null) {
                throw new IllegalArgumentException("toRecordFn already set, you can't use extractKeyFn if it's set");
            }
            this.extractKeyFn = extractKeyFn;
            return this;
        }

        /**
         * TODO [viliam]
         * The default is to use the input item directly.
         *
         * @param extractValueFn a function to extract the value from the sunk item
         * @return this instance for fluent API
         */
        public Builder<E> extractValueFn(@Nonnull FunctionEx<? super E, ?> extractValueFn) {
            if (toRecordFn != null) {
                throw new IllegalArgumentException("toRecordFn already set, you can't use extractValueFn if it's set");
            }
            this.extractValueFn = extractValueFn;
            return this;
        }

        /**
         * Enables or disables the exactly-once behavior of the sink using
         * two-phase commit of state snapshots. If enabled, the {@linkplain
         * JobConfig#setProcessingGuarantee(ProcessingGuarantee) processing
         * guarantee} of the job must be set to {@linkplain
         * ProcessingGuarantee#EXACTLY_ONCE exactly-once}, otherwise the sink's
         * guarantee will match that of the job. In other words, sink's
         * guarantee cannot be higher than job's, but can be lower to avoid the
         * additional overhead.
         * <p>
         * Exactly-once behavior is achieved using Kafka transactions. Records
         * will be only committed after a successful snapshot was done. If a
         * read-committed consumer is used, it will see the records with much
         * higher latency depending on the snapshot interval. The throughput is
         * also very slightly limited because while waiting for all the members
         * doing the snapshot, no more items can be produced to Kafka.
         * <p>
         * When using transactions pay attention to your {@code
         * transaction.timeout.ms} config property. It limits the entire
         * duration of the transaction since it is begun, not just inactivity
         * timeout. It must not be smaller than your snapshot interval,
         * otherwise the Kafka broker will roll back the transaction before Jet
         * is done with it. Also it should be large enough so that Jet has time
         * to restart after a failure: a member can crash just before it's
         * about to commit, and Jet will attempt to commit the transaction
         * after the restart, but the transaction must be still waiting in the
         * broker.
         * <p>
         * The default value is true.
         *
         * @param enable If true, sink's guarantee will match the job
         *      guarantee. If false, sink's guarantee will be at-least-once
         *      even if job's is exactly-once
         * @return this instance for fluent API
         */
        public Builder<E> exactlyOnce(boolean enable) {
            exactlyOnce = enable;
            return this;
        }

        /**
         * TODO [viliam]
         * @return this instance for fluent API
         */
        public Sink<E> build() {
            if ((extractValueFn != null || extractKeyFn != null) && topic == null) {
                throw new IllegalArgumentException("if extractKeyFn or extractValueFn are set, topic must be set too");
            }
            if (topic == null && toRecordFn == null) {
                throw new IllegalArgumentException("either from topic or toRecordFn must be set");
            }
            if (topic != null) {
                FunctionEx<? super E, ?> extractKeyFn1 = this.extractKeyFn != null ? this.extractKeyFn : t -> null;
                FunctionEx<? super E, ?> extractValueFn1 = this.extractValueFn != null ? this.extractValueFn : t -> t;
                return Sinks.fromProcessor("writeKafka(" + topic + ")",
                        writeKafkaP(properties, topic, extractKeyFn1, extractValueFn1, exactlyOnce));
            } else {
                ProcessorMetaSupplier metaSupplier = writeKafkaP(properties, toRecordFn, exactlyOnce);
                return Sinks.fromProcessor("writeKafka", metaSupplier);
            }
        }
    }
}
