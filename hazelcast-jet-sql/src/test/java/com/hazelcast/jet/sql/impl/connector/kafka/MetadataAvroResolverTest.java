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

import com.hazelcast.jet.sql.impl.connector.EntryMetadata;
import com.hazelcast.jet.sql.impl.extract.AvroQueryTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.AvroUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.kafka.MetadataAvroResolver.INSTANCE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class MetadataAvroResolverTest {

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveFields(boolean key, String prefix) {
        List<MappingField> fields = INSTANCE.resolveFields(
                key,
                singletonList(field("field", QueryDataType.INT, prefix + ".field")),
                emptyMap(),
                null
        );

        assertThat(fields).containsExactly(field("field", QueryDataType.INT, prefix + ".field"));
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void when_invalidExternalName_then_throws(boolean key) {
        assertThatThrownBy(() -> INSTANCE.resolveFields(
                key,
                singletonList(field("field", QueryDataType.INT, "does_not_start_with_key_or_value")),
                emptyMap(),
                null
        )).isInstanceOf(QueryException.class);
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_duplicateExternalName_then_throws(boolean key, String prefix) {
        assertThatThrownBy(() -> INSTANCE.resolveFields(
                key,
                asList(
                        field("field1", QueryDataType.INT, prefix + ".field"),
                        field("field2", QueryDataType.VARCHAR, prefix + ".field")
                ),
                emptyMap(),
                null
        )).isInstanceOf(QueryException.class);
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveMetadata(boolean key, String prefix) {
        EntryMetadata metadata = INSTANCE.resolveMetadata(
                key,
                asList(
                        field("string", QueryDataType.VARCHAR, prefix + ".string"),
                        field("boolean", QueryDataType.BOOLEAN, prefix + ".boolean"),
                        field("byte", QueryDataType.TINYINT, prefix + ".byte"),
                        field("short", QueryDataType.SMALLINT, prefix + ".short"),
                        field("int", QueryDataType.INT, prefix + ".int"),
                        field("long", QueryDataType.BIGINT, prefix + ".long"),
                        field("float", QueryDataType.REAL, prefix + ".float"),
                        field("double", QueryDataType.DOUBLE, prefix + ".double"),
                        field("decimal", QueryDataType.DECIMAL, prefix + ".decimal")
                ),
                emptyMap(),
                null
        );

        assertThat(metadata.getFields()).containsExactly(
                new KafkaTableField("string", QueryDataType.VARCHAR, QueryPath.create(prefix + ".string")),
                new KafkaTableField("boolean", QueryDataType.BOOLEAN, QueryPath.create(prefix + ".boolean")),
                new KafkaTableField("byte", QueryDataType.TINYINT, QueryPath.create(prefix + ".byte")),
                new KafkaTableField("short", QueryDataType.SMALLINT, QueryPath.create(prefix + ".short")),
                new KafkaTableField("int", QueryDataType.INT, QueryPath.create(prefix + ".int")),
                new KafkaTableField("long", QueryDataType.BIGINT, QueryPath.create(prefix + ".long")),
                new KafkaTableField("float", QueryDataType.REAL, QueryPath.create(prefix + ".float")),
                new KafkaTableField("double", QueryDataType.DOUBLE, QueryPath.create(prefix + ".double")),
                new KafkaTableField("decimal", QueryDataType.DECIMAL, QueryPath.create(prefix + ".decimal"))
        );
        assertThat(metadata.getQueryTargetDescriptor()).isEqualTo(AvroQueryTargetDescriptor.INSTANCE);
        assertThat(metadata.getUpsertTargetDescriptor())
                .isEqualTo(new AvroUpsertTargetDescriptor(
                                "{"
                                        + "\"type\":\"record\""
                                        + ",\"name\":\"sql\""
                                        + ",\"namespace\":\"jet\""
                                        + ",\"fields\":["
                                        + "{\"name\":\"string\",\"type\":[\"string\",\"null\"],\"default\":null}"
                                        + ",{\"name\":\"boolean\",\"type\":[\"null\",\"boolean\"],\"default\":null}"
                                        + ",{\"name\":\"byte\",\"type\":[\"null\",\"int\"],\"default\":null}"
                                        + ",{\"name\":\"short\",\"type\":[\"null\",\"int\"],\"default\":null}"
                                        + ",{\"name\":\"int\",\"type\":[\"null\",\"int\"],\"default\":null}"
                                        + ",{\"name\":\"long\",\"type\":[\"null\",\"long\"],\"default\":null}"
                                        + ",{\"name\":\"float\",\"type\":[\"null\",\"float\"],\"default\":null}"
                                        + ",{\"name\":\"double\",\"type\":[\"null\",\"double\"],\"default\":null}"
                                        + ",{\"name\":\"decimal\",\"type\":[\"string\",\"null\"],\"default\":null}"
                                        + "]"
                                        + "}"
                        )
                );
    }

    private static MappingField field(String name, QueryDataType type, String externalName) {
        return new MappingField(name, type, externalName);
    }
}
