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

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import java.util.List;
import java.util.Properties;

class KafkaTable extends JetTable {

    private final UpsertTargetDescriptor keyUpsertDescriptor;
    private final UpsertTargetDescriptor valueUpsertDescriptor;

    private final String topicName;
    private final Properties kafkaProperties;

    KafkaTable(
            SqlConnector sqlConnector,
            String schemaName,
            String name,
            TableStatistics statistics,
            String topicName,
            List<TableField> fields,
            UpsertTargetDescriptor keyUpsertDescriptor,
            UpsertTargetDescriptor valueUpsertDescriptor,
            Properties kafkaProperties
    ) {
        super(sqlConnector, fields, schemaName, name, statistics);

        this.keyUpsertDescriptor = keyUpsertDescriptor;
        this.valueUpsertDescriptor = valueUpsertDescriptor;

        this.topicName = topicName;
        this.kafkaProperties = kafkaProperties;
    }

    String getTopicName() {
        return topicName;
    }

    UpsertTargetDescriptor getKeyUpsertDescriptor() {
        return keyUpsertDescriptor;
    }

    UpsertTargetDescriptor getValueUpsertDescriptor() {
        return valueUpsertDescriptor;
    }

    Properties getKafkaProperties() {
        return kafkaProperties;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{topicName=" + topicName + '}';
    }
}
