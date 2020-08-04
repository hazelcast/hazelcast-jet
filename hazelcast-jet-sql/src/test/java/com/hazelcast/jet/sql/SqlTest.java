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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.map.LocalPartitionedMapConnector;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_VALUE_CLASS;
import static java.util.Arrays.asList;

public class SqlTest extends SqlTestSupport {

    @Test
    public void supportsValues() {
        assertRowsEventuallyAnyOrder(
                "SELECT a - b FROM (VALUES (1, 2), (3, 5), (7, 11)) AS t (a, b) WHERE a + b > 4",
                asList(
                        new Row((byte) -2),
                        new Row((byte) -4)
                )
        );
    }

    @Test
    public void supportsCreatingMapFromFile() {
        String name = generateRandomName();

        executeSql(
                "CREATE EXTERNAL TABLE " + name + " ("
                        + "key EXTERNAL NAME \"__key\""
                        + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                        + "OPTIONS ("
                        + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_KEY_CLASS + "\" '" + Integer.class.getName() + "'"
                        + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                        + ") AS "
                        + "SELECT age, username FROM TABLE ("
                        + "FILE ('avro', '" + RESOURCES_PATH + "', 'users.avro')"
                        + ")"
        );

        assertRowsEventuallyAnyOrder(
                "SELECT key, username FROM " + name,
                asList(
                        new Row(0, "User0"),
                        new Row(1, "User1")
                )
        );
    }

    @Test
    public void supportsCreatingMapFromAnotherMap() {
        String sourceName = generateRandomName();
        String destinationName = generateRandomName();

        Map<Integer, String> map = instance().getMap(sourceName);
        map.put(0, "value-0");
        map.put(1, "value-1");

        executeSql(
                "CREATE EXTERNAL TABLE " + destinationName + " ("
                        + "key EXTERNAL NAME \"__key\""
                        + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                        + "OPTIONS ("
                        + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_KEY_CLASS + "\" '" + String.class.getName() + "'"
                        + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_VALUE_CLASS + "\" '" + Long.class.getName() + "'"
                        + ") AS "
                        + "SELECT this AS \"value\", CAST(__key + 1 AS BIGINT) AS id FROM " + sourceName
        );

        assertRowsEventuallyAnyOrder(
                "SELECT key, id FROM " + destinationName,
                asList(
                        new Row("value-0", 1L),
                        new Row("value-1", 2L)
                )
        );
    }

    @Test
    public void supportsCreatingMapFromValues() {
        String name = generateRandomName();

        executeSql(
                "CREATE EXTERNAL TABLE " + name + " ("
                        + "key EXTERNAL NAME \"__key\""
                        + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                        + "OPTIONS ("
                        + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_KEY_CLASS + "\" '" + Byte.class.getName() + "'"
                        + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                        + ") AS "
                        + "VALUES (0, 'value-0'), (1, 'value-1')"
        );

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row((byte) 0, "value-0"),
                        new Row((byte) 1, "value-1")
                )
        );
    }

    private static String generateRandomName() {
        return "m_" + randomString().replace('-', '_');
    }
}
