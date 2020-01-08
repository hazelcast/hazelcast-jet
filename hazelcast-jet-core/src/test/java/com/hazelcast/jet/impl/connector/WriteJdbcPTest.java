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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.ds.common.BaseDataSource;
import org.postgresql.xa.PGXADataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static javax.transaction.xa.XAResource.TMFAIL;
import static org.junit.Assert.assertEquals;

public class WriteJdbcPTest extends SimpleTestInClusterSupport {

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static PostgreSQLContainer container = new PostgreSQLContainer<>("postgres:12.1")
            .withCommand("postgres -c max_prepared_transactions=10");

    private static final int PERSON_COUNT = 10;

    private static AtomicInteger tableCounter = new AtomicInteger();
    private String tableName;

    @BeforeClass
    public static void setupClass() {
        initialize(2, null);
    }

    @Before
    public void setup() throws SQLException {
        tableName = "T" + tableCounter.incrementAndGet();
        logger.info("Will use table: " + tableName);
        try (Connection connection = ((DataSource) createDataSource(false)).getConnection()) {
            connection.createStatement()
                      .execute("CREATE TABLE " + tableName + "(id int primary key, name varchar(255))");
        }
    }

    @Test
    public void test() throws SQLException {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(IntStream.range(0, PERSON_COUNT).boxed().toArray(Integer[]::new)))
         .map(item -> entry(item, item.toString()))
         .writeTo(Sinks.jdbc("INSERT INTO " + tableName + " VALUES(?, ?)",
                 () -> createDataSource(false),
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
                 failTwiceDataSourceSupplier(), failOnceBindFn()
         ));

        instance().newJob(p).join();
        assertEquals(PERSON_COUNT, rowCount());
    }

    @Test(expected = CompletionException.class)
    public void testFailJob_withNonTransientException() throws SQLException {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(IntStream.range(0, PERSON_COUNT).boxed().toArray(Integer[]::new)))
         .map(item -> entry(item, item.toString()))
         .writeTo(Sinks.jdbc("INSERT INTO " + tableName + " VALUES(?, ?)",
                 () -> createDataSource(false),
                 (stmt, item) -> {
                     throw new SQLNonTransientException();
                 }
         ));

        instance().newJob(p).join();
        assertEquals(PERSON_COUNT, rowCount());
    }

    @Test
    public void test2() throws Exception {
        XADataSource dataSource = (XADataSource) createDataSource(true);
        Xid xid1 = new SerializableXID(1, new byte[] {1}, new byte[1]);
        XAConnection conn = dataSource.getXAConnection();
        conn.getConnection().setAutoCommit(false);
        conn.getXAResource().start(xid1, XAResource.TMNOFLAGS);
        PreparedStatement stmt = conn.getConnection().prepareStatement("insert into " + tableName + " values (?, ?)");
        for (int i = 0; i < 10; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "name-" + i);
            stmt.addBatch();
        }
        stmt.executeBatch();
        try {
            conn.getXAResource().end(xid1, TMFAIL);
            conn.getXAResource().rollback(xid1);
            conn.getConnection().close();
        } catch (Exception e) {
            // log and ignore
            e.printStackTrace();
            conn = null;
            System.gc();
            System.gc();
            System.gc();
            System.runFinalization();
        }

        conn = dataSource.getXAConnection();
        conn.getConnection().setAutoCommit(false);
        try {
            conn.getXAResource().rollback(xid1);
        } catch (XAException e) {
            e.printStackTrace();
            if (e.errorCode != XAException.XAER_NOTA) {
                throw e;
            }
        }
        conn.getXAResource().start(xid1, XAResource.TMNOFLAGS);
    }

    @Test
    public void test1() throws Exception {
        XADataSource dataSource = (XADataSource) createDataSource(true);
        Xid xid1 = new SerializableXID(1, new byte[] {1}, new byte[1]);
        Xid xid2 = new SerializableXID(1, new byte[] {2}, new byte[1]);
        XAConnection conn = dataSource.getXAConnection();
        XAConnection conn2 = dataSource.getXAConnection();
        conn.getConnection().setAutoCommit(false);
        conn2.getConnection().setAutoCommit(false);
        try {
            conn.getXAResource().rollback(xid1);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA) {
                throw e;
            }
        }
        try {
            conn2.getXAResource().rollback(xid1);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA) {
                throw e;
            }
        }
        conn.getXAResource().start(xid1, XAResource.TMNOFLAGS);
        PreparedStatement stmt = conn.getConnection().prepareStatement("insert into " + tableName + " values (?, ?)");
        for (int i = 0; i < 10; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "name-" + i);
            stmt.addBatch();
        }
        stmt.executeBatch();
        conn.getConnection().close();
        conn2.getConnection().close();

        conn = dataSource.getXAConnection();
        conn2 = dataSource.getXAConnection();
        conn.getConnection().setAutoCommit(false);
        conn2.getConnection().setAutoCommit(false);
        try {
            conn.getXAResource().rollback(xid1);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA) {
                throw e;
            }
        }
        try {
            conn2.getXAResource().rollback(xid1);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA) {
                throw e;
            }
        }
        conn.getXAResource().start(xid1, XAResource.TMNOFLAGS);
        stmt = conn.getConnection().prepareStatement("insert into " + tableName + " values (?, ?)");
        for (int i = 0; i < 10; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "name-" + i);
            stmt.addBatch();
        }
        stmt.executeBatch();
    }

    // TODO [viliam] more tests

    @Test
    public void test_transactional_withRestarts_graceful() throws Exception {
        test_transactional_withRestarts(true);
    }

    @Test
    public void test_transactional_withRestarts_forceful() throws Exception {
        test_transactional_withRestarts(false);
    }

    private void test_transactional_withRestarts(boolean graceful) throws Exception {
        // TODO [viliam] make this test faster
        int numItems = 1000;
        Pipeline p = Pipeline.create();
        p.readFrom(SourceBuilder.stream("src", procCtx -> new int[1])
                                .<Integer>fillBufferFn((ctx, buf) -> {
                                    if (ctx[0] < numItems) {
                                        buf.add(ctx[0]++);
                                        sleepMillis(10);
                                    }
                                })
                                .createSnapshotFn(ctx -> ctx[0])
                                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                                .build())
         .withoutTimestamps()
         .peek()
         .writeTo(Sinks.jdbc("INSERT INTO " + tableName + " VALUES(?, ?)",
                 () -> createDataSource(true),
                 (stmt, item) -> {
                     stmt.setInt(1, item);
                     stmt.setString(2, "name-" + item);
                 }
         ));

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        JobProxy job = (JobProxy) instance().newJob(p, config);

        try (Connection conn = ((DataSource) createDataSource(false)).getConnection();
             PreparedStatement stmt = conn.prepareStatement("select id from " + tableName)
        ) {
            long endTime = System.nanoTime() + SECONDS.toNanos(20); // TODO [viliam] longer timeout
            int lastCount = 0;
            String expectedRows = IntStream.range(0, numItems).mapToObj(Integer::toString).collect(joining("\n"));
            SortedSet<Integer> actualRows = new TreeSet<>();
            // We'll restart once, then restart again after a short sleep (possibly during initialization), then restart
            // again and then assert some output so that the test isn't constantly restarting without any progress
            for (;;) {
                assertJobStatusEventually(job, RUNNING);
                job.restart(graceful);
                assertJobStatusEventually(job, RUNNING);
                sleepMillis(ThreadLocalRandom.current().nextInt(400));
                job.restart(graceful);
                try {
                    do {
                        actualRows.clear();
                        ResultSet resultSet = stmt.executeQuery();
                        while (resultSet.next()) {
                            actualRows.add(resultSet.getInt(1));
                        }
                    } while (actualRows.size() < Math.min(numItems, 100 + lastCount)
                            && System.nanoTime() < endTime);
                    lastCount = actualRows.size();
                    logger.info("number of committed items in the sink so far: " + lastCount);
                    assertEquals(expectedRows, actualRows.stream().map(Objects::toString).collect(joining("\n")));
                    // if content matches, break the loop. Otherwise restart and try again
                    break;
                } catch (AssertionError e) {
                    if (System.nanoTime() >= endTime) {
                        throw e;
                    }
                }
            }
        } finally {
            // We have to remove the job before bringing down Kafka broker because
            // the producer can get stuck otherwise.
            ditchJob(job, instances());
        }
    }

    private int rowCount() throws SQLException {
        try (Connection connection = ((DataSource) createDataSource(false)).getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + tableName);
            if (!resultSet.next()) {
                return 0;
            }
            return resultSet.getInt(1);
        }
    }

    private static CommonDataSource createDataSource(boolean xa) {
        BaseDataSource dataSource = xa ? new PGXADataSource() : new PGSimpleDataSource();
        dataSource.setUrl(container.getJdbcUrl());
        dataSource.setUser(container.getUsername());
        dataSource.setPassword(container.getPassword());
        dataSource.setDatabaseName(container.getDatabaseName());

        return dataSource;
    }

    private static SupplierEx<DataSource> failTwiceDataSourceSupplier() {
        return new SupplierEx<DataSource>() {
            int remainingFailures = 2;

            @Override
            public DataSource getEx() throws SQLException {
                if (remainingFailures-- > 0) {
                    throw new SQLException("connection failure");
                }
                return ((DataSource) createDataSource(false));
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
