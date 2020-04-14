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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlWriters.EntryWriter;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.schema.Table;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * {@link Table} implementation for IMap.
 */
public class KafkaTable extends JetTable {

    private final String topicName;
    private final EntryWriter writer;
    private final Properties kafkaProperties;

    public KafkaTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull String topicName,
            @Nonnull List<Entry<String, QueryDataType>> fields,
            @Nonnull EntryWriter writer,
            @Nonnull Properties kafkaProperties
    ) {
        super(sqlConnector, fields);
        this.topicName = topicName;
        this.writer = writer;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public boolean isStream() {
        return true;
    }

    public String getTopicName() {
        return topicName;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + topicName + '}';
    }

    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

    public EntryWriter getWriter() {
        return writer;
    }
}
