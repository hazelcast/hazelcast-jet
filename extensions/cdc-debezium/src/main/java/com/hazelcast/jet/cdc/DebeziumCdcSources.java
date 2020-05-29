/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.impl.CdcSource;
import com.hazelcast.jet.cdc.impl.ConstantSequenceExtractor;
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for creating Change Data Capture (CDC) sources.
 * <p>
 * Note: It is better to use first-class CDC sources than this generic one
 * because it has less functionality. For example, these sources lack
 * sequence numbers, so functionality based on them (like reordering
 * protection in {@link CdcSinks}) is disabled.
 *
 * @since 4.2
 */
@EvolvingApi
public final class DebeziumCdcSources {

    private DebeziumCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a
     * Debezium-supported database to a Hazelcast Jet pipeline.
     *
     * @param name the name of this source, must unique, will be passed to the underlying
     *             Kafka Connect source
     * @return a builder you can use to set the source's properties and then construct it
     */
    @Nonnull
    public static Builder debezium(@Nonnull String name, @Nonnull String connectorClass) {
        return new Builder(name, connectorClass);
    }

    /**
     * A builder to configure a CDC source that streams the change data from
     * a Debezium-supported database to Hazelcast Jet.
     */
    public static final class Builder {

        private final DebeziumConfig config;

        /**
         * @param name name of the source, needs to be unique, will be passed to the underlying
         *             Kafka Connect source
         * @param connectorClass name of the Java class for the connector, hardcoded for each type of DB
         */
        private Builder(String name, String connectorClass) {
            config = new DebeziumConfig(name, connectorClass);
            config.setProperty(CdcSource.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, ConstantSequenceExtractor.class.getName());
        }

        /**
         * Sets a source property. These properties are passed to Debezium.
         */
        @Nonnull
        public Builder setProperty(@Nonnull String key, @Nonnull String value) {
            config.setProperty(key, value);
            return this;
        }

        /**
         * Returns the CDC source based on the properties set.
         */
        @Nonnull
        public StreamSource<ChangeRecord> build() {
            return config.createSource();
        }
    }

}
