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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.sql.JetSqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_OBJECT_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlPrimitiveTest extends JetSqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_insertIntoDiscoveredMap() {
        String name = generateRandomName();

        instance().getMap(name).put(BigInteger.valueOf(1), "Alice");

        assertMapEventually(
                name,
                "SINK INTO partitioned." + name + " VALUES (2, 'Bob')",
                createMap(BigInteger.valueOf(1), "Alice", BigInteger.valueOf(2), "Bob")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(BigDecimal.valueOf(1), "Alice"),
                        new Row(BigDecimal.valueOf(2), "Bob")
                )
        );
    }

    @Test
    public void test_insertSelect() {
        String name = generateRandomName();
        sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));

        IMap<Integer, String> source = instance().getMap("source");
        source.put(0, "value-0");
        source.put(1, "value-1");

        assertMapEventually(
                name,
                "SINK INTO " + name + " SELECT * FROM " + source.getName(),
                createMap(0, "value-0", 1, "value-1")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(0, "value-0"),
                        new Row(1, "value-1")
                )
        );
    }

    @Test
    public void test_insertValues() {
        String name = generateRandomName();
        sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));

        assertMapEventually(
                name,
                "SINK INTO " + name + " (this, __key) VALUES ('2', 1)",
                createMap(1, "2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_insertWithProject() {
        String name = generateRandomName();
        sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));

        assertMapEventually(
                name,
                "SINK INTO " + name + " (this, __key) VALUES (2, CAST(0 + 1 AS INT))",
                createMap(1, "2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = generateRandomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME __key"
                + ", name VARCHAR EXTERNAL NAME this"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", " + OPTION_KEY_CLASS + " '" + Integer.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                + ")"
        );

        assertMapEventually(
                name,
                format("SINK INTO %s (id, name) VALUES (2, 'value-2')", name),
                createMap(2, "value-2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(2, "value-2"))
        );
    }

    @Test
    public void test_mapNameAndTableNameDifferent() {
        String mapName = generateRandomName();
        String tableName = generateRandomName();

        sqlService.execute("CREATE MAPPING " + tableName + " TYPE " + IMapSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_OBJECT_NAME + " '" + mapName + "'"
                + ", " + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", " + OPTION_KEY_CLASS + " '" + String.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                + ")"
        );

        IMap<String, String> map = instance().getMap(mapName);
        map.put("k1", "v1");
        map.put("k2", "v2");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + tableName,
                asList(
                        new Row("k1", "v1"),
                        new Row("k2", "v2")
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + tableName,
                asList(
                        new Row("k1", "v1"),
                        new Row("k2", "v2")
                )
        );
    }

    @Test
    public void when_insertInto_then_throws() {
        String name = generateRandomName();
        sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));

        assertThatThrownBy(() -> sqlService.execute("INSERT INTO " + name + " (__key, this) VALUES (1, '2')"))
                .hasMessageContaining("INSERT INTO clause is not supported for IMap");
    }

    @Test
    public void when_typeMismatch_then_fail() {
        String name = generateRandomName();
        instance().getMap(name).put(0, 0);
        sqlService.execute(javaSerializableMapDdl(name, String.class, String.class));

        assertThatThrownBy(() -> sqlService.execute("SELECT __key FROM " + name).iterator().forEachRemaining(row -> { }))
                .hasMessageContaining("Failed to extract map entry key because of type mismatch " +
                        "[expectedClass=java.lang.String, actualClass=java.lang.Integer]");
    }

    private static String generateRandomName() {
        return "primitive_" + randomString().replace('-', '_');
    }
}
