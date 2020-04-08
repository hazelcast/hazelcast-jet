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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.cdc.HazelcastListDatabaseHistory;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import java.util.Properties;

public abstract class AbstractSourceBuilder<SELF extends AbstractSourceBuilder<SELF>> {

    protected final Properties properties = new Properties();

    /**
     * @param name           name of the source, needs to be unique,
     *                       will be passed to the underlying Kafka
     *                       Connect source
     * @param connectorClass name of the Java class for the connector,
     *                       hardcoded for each type of DB
     */
    protected AbstractSourceBuilder(String name, String connectorClass) {
        properties.put("name", name);
        properties.put("connector.class", connectorClass);
        properties.put("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.put("database.history.hazelcast.list.name", name);
        properties.put("tombstones.on.delete", "false");
    }

    /**
     * Can be used to set any property not covered by our builders,
     * or to override properties we have hidden.
     *
     * @param key   the name of the property to set
     * @param value the value of the property to set
     * @return the builder itself
     */
    public SELF setCustomProperty(String key, String value) {
        properties.put(key, value);
        return (SELF) this;
    }

    protected SELF setProperty(String key, String value) {
        properties.put(key, value);
        return (SELF) this;
    }

    protected SELF setProperty(String key, int value) {
        return setProperty(key, Integer.toString(value));
    }

    protected SELF setProperty(String key, boolean value) {
        return setProperty(key, Boolean.toString(value));
    }

    protected SELF setProperty(String key, String... values) {
        return setProperty(key, String.join(",", values));
    }

    protected static StreamSource<ChangeEvent> connect(
            @Nonnull Properties properties,
            @Nonnull FunctionEx<ChangeEvent, Long> eventToTimestampMapper,
            @Nonnull FunctionEx<SourceRecord, ChangeEvent> recordToEventMapper) {
        String name = properties.getProperty("name");
        FunctionEx<Processor.Context, KafkaConnectSource> createFn = ctx -> new KafkaConnectSource(ctx, properties,
                recordToEventMapper, eventToTimestampMapper);
        return SourceBuilder.timestampedStream(name, createFn)
                .fillBufferFn(KafkaConnectSource::fillBuffer)
                .createSnapshotFn(KafkaConnectSource::createSnapshot)
                .restoreSnapshotFn(KafkaConnectSource::restoreSnapshot)
                .destroyFn(KafkaConnectSource::destroy)
                .build();
    }

}
