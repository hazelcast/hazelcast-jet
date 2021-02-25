/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class MappingColumnsTableTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    public void test_rows() {
        // given
        Table table = new JetTable(
                null,
                asList(
                        new TableField("table-field-name", INT, false),
                        new TableField("this", OBJECT, true)
                ),
                "table-schema",
                "table-name",
                null
        );
        Mapping mapping = new Mapping(
                "table-name",
                "table-external-name",
                "table-type",
                singletonList(new MappingField("table-field-name", INT, "table-field-external-name")),
                emptyMap()
        );
        MappingDefinition definition = new MappingDefinition(table, mapping);

        MappingColumnsTable mappingColumnsTable = new MappingColumnsTable("catalog", null, singletonList(definition));

        // when
        List<Object[]> rows = mappingColumnsTable.rows();

        // then
        assertThat(rows).containsExactly(
                new Object[]{
                        "catalog",
                        "table-schema",
                        "table-name",
                        "table-field-name",
                        "table-field-external-name",
                        0,
                        "true",
                        "INTEGER"
                },
                new Object[]{
                        "catalog",
                        "table-schema",
                        "table-name",
                        "this",
                        null,
                        1,
                        "true",
                        "OBJECT"
                }
        );
    }
}
