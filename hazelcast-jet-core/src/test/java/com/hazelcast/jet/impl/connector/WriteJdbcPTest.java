/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.util.Map.Entry;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static org.junit.Assert.assertEquals;

public class WriteJdbcPTest extends SimpleTestInClusterSupport {

    private static final int PERSON_COUNT = 10;

    private static String dbConnectionUrl;
    private static AtomicInteger tableCounter = new AtomicInteger();
    private String tableName = "T" + tableCounter.incrementAndGet();

    @BeforeClass
    public static void setupClass() {
        initialize(2, null);
        dbConnectionUrl = "jdbc:h2:mem:" + WriteJdbcPTest.class.getSimpleName() + ";DB_CLOSE_DELAY=-1";
    }

    @AfterClass
    public static void deleteDbFiles() throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl)) {
            conn.createStatement().execute("shutdown");
        }
    }

    @Before
    public void setup() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbConnectionUrl)) {
            connection.createStatement()
                      .execute("CREATE TABLE " + tableName + "(id int primary key, name varchar(255))");
        }
    }

    @Test
    public void test() throws SQLException {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(IntStream.range(0, PERSON_COUNT).boxed().toArray(Integer[]::new)))
         .map(item -> entry(item, item.toString()))
         .writeTo(Sinks.jdbc("INSERT INTO " + tableName + " VALUES(?, ?)", dbConnectionUrl,
                 (stmt, item) -> {
                     stmt.setInt(1, item.getKey());
                     stmt.setString(2, item.getValue());
                 }
         ));

        instance().newJob(p).join();
        assertEquals(PERSON_COUNT, rowCount());
    }

    @Test
    public void testReconnect() throws SQLException {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(IntStream.range(0, PERSON_COUNT).boxed().toArray(Integer[]::new)))
         .map(item -> entry(item, item.toString()))
         .writeTo(Sinks.jdbc("INSERT INTO " + tableName + " VALUES(?, ?)",
                 failTwiceConnectionSupplier(), failOnceBindFn()
         ));

        instance().newJob(p).join();
        assertEquals(PERSON_COUNT, rowCount());
    }

    @Test(expected = CompletionException.class)
    public void testFailJob_withNonTransientException() throws SQLException {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(IntStream.range(0, PERSON_COUNT).boxed().toArray(Integer[]::new)))
         .map(item -> entry(item, item.toString()))
         .writeTo(Sinks.jdbc("INSERT INTO " + tableName + " VALUES(?, ?)", dbConnectionUrl,
                 (stmt, item) -> {
                     throw new SQLNonTransientException();
                 }
         ));

        instance().newJob(p).join();
        assertEquals(PERSON_COUNT, rowCount());
    }

    private int rowCount() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbConnectionUrl);
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + tableName);
            if (!resultSet.next()) {
                return 0;
            }
            return resultSet.getInt(1);
        }
    }

    private static SupplierEx<Connection> failTwiceConnectionSupplier() {
        return new SupplierEx<Connection>() {
            int remainingFailures = 2;

            @Override
            public Connection getEx() throws SQLException {
                if (remainingFailures-- > 0) {
                    throw new SQLException("connectionFailure");
                }
                return DriverManager.getConnection(dbConnectionUrl);
            }
        };
    }

    private static BiConsumerEx<PreparedStatement, Entry<Integer, String>> failOnceBindFn() {
        return new BiConsumerEx<PreparedStatement, Entry<Integer, String>>() {
            int remainingFailures = 1;

            @Override
            public void acceptEx(PreparedStatement stmt, Entry<Integer, String> item) throws SQLException {
                if (remainingFailures-- > 0) {
                    throw new SQLException("bindFn failure");
                }
                stmt.setInt(1, item.getKey());
                stmt.setString(2, item.getValue());
            }
        };
    }
}
