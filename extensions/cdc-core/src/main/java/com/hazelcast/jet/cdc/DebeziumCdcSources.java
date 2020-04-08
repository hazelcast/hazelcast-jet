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

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.impl.AbstractSourceBuilder;
import com.hazelcast.jet.cdc.impl.ChangeEventJsonImpl;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.connect.data.Values;

/**
 * Contains factory methods for creating change data capture sources
 *
 * @since 4.1
 */
@EvolvingApi
public final class DebeziumCdcSources {

    //todo: can we use cdc sources in a distributed way?

    //todo: update main README.md files in cdc modules

    private DebeziumCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from your Debezium
     * supported database to the Hazelcast Jet pipeline.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also
     * to construct the source once configuration is done
     */
    public static Builder debezium(String name, String connectorClass) {
        return new Builder(name, connectorClass);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from any Debezium supported database to Hazelcast Jet.
     */
    public static final class Builder extends AbstractSourceBuilder<Builder> {

        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        private Builder(String name, String connectorClass) {
            super(name, connectorClass);
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
            return connect(properties,
                    (event) -> event.value().getLong("ts_ms").orElse(0L),
                    (record) -> {
                        String keyJson = Values.convertToString(record.keySchema(), record.key());
                        String valueJson = Values.convertToString(record.valueSchema(), record.value());
                        return new ChangeEventJsonImpl(keyJson, valueJson);
                    }
            );
        }
    }

}
