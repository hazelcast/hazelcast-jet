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
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqlLimitTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void limitOverTable() {
        String tableName = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertContainsOnlyOneOfRows(
                "SELECT name FROM " + tableName + " LIMIT 1",
                new Row("Alice"), new Row("Bob"), new Row("Joey")
        );

        assertContainsSubsetOfRows(
                "SELECT name FROM " + tableName + " LIMIT 2",
                2,
                new Row("Alice"), new Row("Bob"), new Row("Joey")
        );

        assertRowsAnyOrder(
                "SELECT name FROM " + tableName + " LIMIT 5",
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
        );
    }

    @Test
    public void negativeLimitValue() {
        String tableName = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        checkFailure0("SELECT name FROM " + tableName + " LIMIT -10", SqlErrorCode.PARSING, "Encountered \"-\"");
    }

    @Test
    public void floatNumber_asLimitValue() {
        String tableName = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name FROM " + tableName + " LIMIT 5.2",
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
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
    public void limitOverStream() {
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT 1",
                Collections.singletonList(new Row(0L))
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT 2",
                asList(
                        new Row(0L),
                        new Row(1L)
                )
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT 10",
                asList(
                        new Row(0L),
                        new Row(1L),
                        new Row(2L),
                        new Row(3L),
                        new Row(4L)
                )
        );
    }

    protected void checkFailure0(
            String sql,
            int expectedErrorCode,
            String expectedErrorMessage,
            Object... params
    ) {
        try {
            SqlStatement statement = new SqlStatement(sql);
            statement.setParameters(asList(params));
            sqlService.execute(statement);

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            assertTrue(expectedErrorMessage.length() != 0);
            assertNotNull(e.getMessage());
            assertTrue(
                    "\nExpected: " + expectedErrorMessage + "\nActual: " + e.getMessage(),
                    e.getMessage().contains(expectedErrorMessage)
            );

            assertEquals(e.getCode() + ": " + e.getMessage(), expectedErrorCode, e.getCode());
        }
    }
}
