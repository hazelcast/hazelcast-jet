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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SqlLimitTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void simple_limit() {
        String tableName = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name FROM " + tableName + " LIMIT 1",
                singletonList(new Row("Alice"))
        );
    }

    private static String createTable(String[]... values) {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(QueryDataType.VARCHAR, QueryDataType.INT),
                asList(values)
        );
        return name;
    }

    @Test
    public void limit_map() throws Exception {
        IMap<Object, Object> map = instance().getMap("map");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        SqlResult execute = instance().getSql().execute("select * from map limit 1");
        execute.iterator().forEachRemaining(row -> System.out.println("ROW: " + row.getObject(0)));
    }
}
