/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.jet.pipeline.Sinks;
import org.h2.tools.DeleteDbFiles;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertEquals;

public class WriteJdbcPTest extends PipelineTestSupport {

    private static final String DIR = "~";
    private static final String DB = WriteJdbcPTest.class.getSimpleName();
    private static final String DB_CONNECTION_URL = "jdbc:h2:" + DIR + "/" + DB;
    private static final int PERSON_COUNT = 10;

    @Rule public TestName testName = new TestName();

    private String tableName;

    @BeforeClass
    public static void setupClass() {
        DeleteDbFiles.execute(DIR, DB, true);
    }

    @Before
    public void setup() throws SQLException {
        tableName = testName.getMethodName().replaceAll("\\[.*?\\]", "") + "_" + testMode.toString();
        createTable();
    }

    @Test
    public void test() throws SQLException {
        addToSrcList(sequence(PERSON_COUNT));
        p.drawFrom(source)
         .map(item -> new Person((Integer) item, item.toString()))
         .drainTo(Sinks.jdbc("INSERT INTO " + tableName + "(id, name) VALUES(?, ?)", DB_CONNECTION_URL,
                 (stmt, item) -> {
                     try {
                         stmt.setInt(1, item.id);
                         stmt.setString(2, item.name);
                     } catch (SQLException e) {
                         throw rethrow(e);
                     }
                 }
         ));

        execute();

        assertEquals(PERSON_COUNT, rowCount());
    }

    @Test
    public void testReconnect() throws SQLException {
        addToSrcList(sequence(PERSON_COUNT));
        p.drawFrom(source)
         .map(item -> new Person((Integer) item, item.toString()))
         .drainTo(Sinks.jdbc("INSERT INTO " + tableName + "(id, name) VALUES(?, ?)",
                 failOnceConnectionSupplier(), failOnceBindFn()
         ));

        execute();

        assertEquals(PERSON_COUNT, rowCount());
    }

    @Test(expected = ExecutionException.class)
    public void testFailJob_withNonTransientException() {
        addToSrcList(sequence(PERSON_COUNT));
        p.drawFrom(source)
         .map(item -> new Person((Integer) item, item.toString()))
         .drainTo(Sinks.jdbc("INSERT INTO " + tableName + "(id, name) VALUES(?, ?)", DB_CONNECTION_URL,
                 (stmt, item) -> rethrow(new SQLNonTransientException())
         ));

        execute();
    }

    private void createTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(DB_CONNECTION_URL);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + tableName + "(id int primary key, name varchar(255))");
        }
    }

    private int rowCount() throws SQLException {
        try (Connection connection = DriverManager.getConnection(DB_CONNECTION_URL);
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT COUNT(id) FROM " + tableName);
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    private static DistributedSupplier<Connection> failOnceConnectionSupplier() {
        return new DistributedSupplier<Connection>() {
            boolean exceptionThrown;
            @Override
            public Connection get() {
                try {
                    if (exceptionThrown) {
                        exceptionThrown = true;
                        throw new SQLException();
                    }
                    return DriverManager.getConnection(DB_CONNECTION_URL);
                } catch (SQLException e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        };
    }

    private static DistributedBiConsumer<PreparedStatement, Person> failOnceBindFn() {
        return new DistributedBiConsumer<PreparedStatement, Person>() {
            boolean exceptionThrown;
            @Override
            public void accept(PreparedStatement stmt, Person item) {
                try {
                    if (exceptionThrown) {
                        exceptionThrown = true;
                        throw new SQLException();
                    }
                    stmt.setInt(1, item.id);
                    stmt.setString(2, item.name);
                } catch (SQLException e) {
                    throw rethrow(e);
                }
            }
        };
    }

    private static final class Person implements Serializable {

        private final int id;
        private final String name;

        private Person(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
