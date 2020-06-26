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
import com.hazelcast.sql.impl.connector.LocalPartitionedMapConnector;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// TODO: move it to IMDG when INSERTs are supported, or at least move to one of Jet connector tests ?
public class SqlInsertTest extends SqlTestSupport {

    private String sourceMapName;
    private String sinkMapName;

    @Before
    public void before() {
        sourceMapName = createRandomMap();
        sinkMapName = createRandomMap();
    }

    @Test
    public void insert_select() {
        Map<Integer, String> intToStringMap = instance().getMap(sourceMapName);
        intToStringMap.put(0, "value-0");
        intToStringMap.put(1, "value-1");

        assertMapEventually(
                sinkMapName,
                format("INSERT OVERWRITE %s SELECT * FROM %s", sinkMapName, sourceMapName),
                createMap(0, "value-0", 1, "value-1"));
    }

    @Test
    public void insert_values() {
        assertMapEventually(
                sinkMapName,
                format("INSERT OVERWRITE %s (this, __key) VALUES ('2', 1), ('4', 3)", sinkMapName),
                createMap(1, "2", 3, "4"));
    }

    @Test
    public void insert_withProject() {
        assertMapEventually(
                sinkMapName,
                format("INSERT OVERWRITE %s (__key, this) VALUES (0 + 1, 2)", sinkMapName),
                createMap(1, "2"));
    }

    @Test
    public void insert_intoMapWithoutOverwriteFails() {
        assertThatThrownBy(
                () -> executeSql(format("INSERT INTO %s (__key, this) VALUES (1, '2')", sinkMapName))
        ).hasMessageContaining("Only INSERT OVERWRITE clause is supported for IMapSqlConnector");
    }

    private static String createRandomMap() {
        String name = generateRandomName();
        executeSql(
                format("CREATE EXTERNAL TABLE %s (" +
                                " __key INT," +
                                " this VARCHAR" +
                                ") TYPE \"%s\"",
                        name, LocalPartitionedMapConnector.TYPE_NAME
                )
        );
        return name;
    }

    private static String generateRandomName() {
        return "m_" + randomString().replace('-', '_');
    }
}
