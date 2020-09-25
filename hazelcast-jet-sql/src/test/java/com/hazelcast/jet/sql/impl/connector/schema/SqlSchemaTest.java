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

package com.hazelcast.jet.sql.impl.connector.schema;

import com.hazelcast.jet.sql.JetSqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.sql.SqlService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SqlSchemaTest extends JetSqlTestSupport {

    private static SqlService sqlService;

    private String name;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Before
    public void setUp() {
        name = generateRandomName();
        sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));
    }

    @Test
    public void test_mappings() {
        // when
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM information_schema.mappings",
                singletonList(
                        new Row(
                                "hazelcast",
                                "public",
                                name,
                                IMapSqlConnector.TYPE_NAME,
                                "{"
                                        + "keyFormat=java"
                                        + ", keyJavaClass=java.lang.Integer"
                                        + ", valueFormat=java"
                                        + ", valueJavaClass=java.lang.String"
                                        + "}")
                )
        );
    }

    @Test
    public void test_columns() {
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM information_schema.columns",
                asList(
                        new Row("hazelcast", "public", name, "__key", "0", "true", "INT"),
                        new Row("hazelcast", "public", name, "this", "1", "true", "VARCHAR")
                )
        );
    }

    @Test
    public void when_predicateAndProjectionIsUsed_then_correctRowsAndColumnsAreReturned() {
        assertRowsEventuallyInAnyOrder(
                "SELECT mapping_name, UPPER(mapping_catalog), column_name, data_type "
                        + "FROM columns "
                        + "WHERE column_name = 'this'",
                singletonList(
                        new Row(name, "HAZELCAST", "this", "VARCHAR")
                )
        );
    }

    private static String generateRandomName() {
        return "schema_" + randomString().replace('-', '_');
    }
}
