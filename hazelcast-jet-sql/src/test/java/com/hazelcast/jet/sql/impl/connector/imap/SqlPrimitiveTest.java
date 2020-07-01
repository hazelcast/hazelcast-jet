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

package com.hazelcast.jet.sql.impl.connector.imap;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.connector.LocalPartitionedMapConnector;
import org.junit.Test;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.sql.impl.connector.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// TODO: move it to IMDG when INSERTs are supported, or at least move to one of Jet connector tests ?
public class SqlPrimitiveTest extends SqlTestSupport {

    @Test
    public void supportsInsertSelect() {
        String name = createTableWithRandomName();

        IMap<Integer, String> source = instance().getMap("source");
        source.put(0, "value-0");
        source.put(1, "value-1");

        assertMapEventually(
                name,
                format("INSERT OVERWRITE %s SELECT * FROM %s", name, source.getName()),
                createMap(0, "value-0", 1, "value-1")
        );
    }

    @Test
    public void supportsInsertValues() {
        String name = createTableWithRandomName();

        assertMapEventually(
                name,
                format("INSERT OVERWRITE %s (this, __key) VALUES ('2', 1), ('4', 3)", name),
                createMap(1, "2", 3, "4")
        );
    }

    @Test
    public void supportsInsertWithProject() {
        String name = createTableWithRandomName();

        assertMapEventually(
                name,
                format("INSERT OVERWRITE %s (__key, this) VALUES (CAST(0 + 1 AS INT), 2)", name),
                createMap(1, "2")
        );
    }

    @Test
    public void supportsFieldsMapping() {
        String name = generateRandomName();

        executeSql(format("CREATE EXTERNAL TABLE %s (" +
                        " id INT EXTERNAL NAME __key," +
                        " name VARCHAR EXTERNAL NAME this" +
                        ") TYPE \"%s\" " +
                        "OPTIONS (" +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'" +
                        ")",
                name, LocalPartitionedMapConnector.TYPE_NAME,
                TO_SERIALIZATION_KEY_FORMAT, JAVA_SERIALIZATION_FORMAT,
                TO_KEY_CLASS, Integer.class.getName(),
                TO_SERIALIZATION_VALUE_FORMAT, JAVA_SERIALIZATION_FORMAT,
                TO_VALUE_CLASS, String.class.getName()
        ));

        assertMapEventually(
                name,
                format("INSERT OVERWRITE %s (name, id) VALUES ('value-2', 2)", name),
                createMap(2, "value-2")
        );
    }

    @Test
    public void supportsOnlyInsertOverwrite() {
        String name = createTableWithRandomName();

        assertThatThrownBy(
                () -> executeSql(format("INSERT INTO %s (__key, this) VALUES (1, '2')", name))
        ).hasMessageContaining("Only INSERT OVERWRITE clause is supported for IMapSqlConnector");
    }

    private static String createTableWithRandomName() {
        String name = generateRandomName();
        executeSql(
                format("CREATE EXTERNAL TABLE %s " +
                                "TYPE \"%s\" " +
                                "OPTIONS (" +
                                " \"%s\" '%s'," +
                                " \"%s\" '%s'," +
                                " \"%s\" '%s'," +
                                " \"%s\" '%s'" +
                                ")",
                        name, LocalPartitionedMapConnector.TYPE_NAME,
                        TO_SERIALIZATION_KEY_FORMAT, JAVA_SERIALIZATION_FORMAT,
                        TO_KEY_CLASS, Integer.class.getName(),
                        TO_SERIALIZATION_VALUE_FORMAT, JAVA_SERIALIZATION_FORMAT,
                        TO_VALUE_CLASS, String.class.getName()
                )
        );
        return name;
    }

    private static String generateRandomName() {
        return "primitive_" + randomString().replace('-', '_');
    }
}
