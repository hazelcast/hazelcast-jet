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

package com.hazelcast.jet.core.processor;

import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.connector.kafka.StreamKafkaP;
import com.hazelcast.jet.impl.connector.kafka.WriteKafkaP;
import com.hazelcast.util.Preconditions;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Static utility class with factories of Apache Kafka source and sink
 * processors.
 */
public final class KafkaProcessors {

    private KafkaProcessors() {
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.KafkaSources#kafka(Properties,
     * DistributedBiFunction, WatermarkGenerationParams, String...)}.
     */
    public static <K, V, T> ProcessorMetaSupplier streamKafkaP(
            @Nonnull Properties properties,
            @Nonnull DistributedBiFunction<K, V, T> projectionFn,
            @Nonnull WatermarkGenerationParams<T> wmGenParams,
            @Nonnull String... topics
    ) {
        Preconditions.checkPositive(topics.length, "At least one topic must be supplied");
        properties.put("enable.auto.commit", false);
        return new StreamKafkaP.MetaSupplier<>(properties, Arrays.asList(topics), projectionFn, wmGenParams);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.KafkaSources#kafka(Properties,
     * WatermarkGenerationParams, String...)}.
     */
    public static <K, V> ProcessorMetaSupplier streamKafkaP(
            @Nonnull Properties properties,
            @Nonnull WatermarkGenerationParams<Entry<K, V>> wmGenParams,
            @Nonnull String... topics
    ) {
        return KafkaProcessors.<K, V, Entry<K, V>>streamKafkaP(properties, Util::entry, wmGenParams, topics);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.KafkaSinks#kafka(Properties, String, DistributedFunction, DistributedFunction)}.
     */
    public static <T, K, V> ProcessorMetaSupplier writeKafkaP(
            @Nonnull Properties properties,
            @Nonnull String topic,
            @Nonnull DistributedFunction<? super T, K> extractKeyFn,
            @Nonnull DistributedFunction<? super T, V> extractValueFn
    ) {
        return writeKafkaP(properties, (T t) ->
                new ProducerRecord<>(topic, extractKeyFn.apply(t), extractValueFn.apply(t))
        );
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.KafkaSinks#kafka(Properties, DistributedFunction)}.
     */
    public static <T, K, V> ProcessorMetaSupplier writeKafkaP(
            @Nonnull Properties properties,
            @Nonnull DistributedFunction<? super T, ProducerRecord<K, V>> toRecordFn
    ) {
        return ProcessorMetaSupplier.of(new WriteKafkaP.Supplier<T, K, V>(properties, toRecordFn), 2);
    }
}
