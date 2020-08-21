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

package com.hazelcast.jet.cdc.mysql;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.Parameter;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SerialTest.class, NightlyTest.class})
public class MySqlCdcNetworkIntegrationTest extends AbstractCdcIntegrationTest {

    private static final long RECONNECT_INTERVAL_MS = SECONDS.toMillis(1);

    @Parameter(value = 0)
    public RetryStrategy reconnectBehaviour;

    @Parameter(value = 1)
    public boolean resetStateOnReconnect;

    @Parameter(value = 2)
    public String testName;

    @Parameters(name = "{2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { RetryStrategies.never(), false, "fail"},
                { RetryStrategies.indefinitely(RECONNECT_INTERVAL_MS), false, "reconnect"},
                { RetryStrategies.indefinitely(RECONNECT_INTERVAL_MS), true, "reconnect w/ state reset"}
        });
    }

    @Test
    public void when_noDatabaseToConnectTo() {
        Pipeline pipeline = initPipeline("localhost", MYSQL_PORT);

        // when job starts
        JetInstance jet = createJetMembers(2)[0];
        Job job = jet.newJob(pipeline);

        boolean neverReconnect = reconnectBehaviour.getMaxAttempts() == 0;
        if (neverReconnect) {
            // then job fails
            assertThatThrownBy(job::join)
                    .hasRootCauseInstanceOf(JetException.class)
                    .hasStackTraceContaining("Failed connecting to database");
            assertTrue(jet.getMap("results").isEmpty());
        } else {
            // and can't connect to DB
            assertJobStatusEventually(job, RUNNING);
            assertTrueAllTheTime(() -> assertTrue(jet.getMap("results").isEmpty()),
                    2 * MILLISECONDS.toSeconds(RECONNECT_INTERVAL_MS));

            // and DB starts
            MySQLContainer<?> mysql = initMySql(null, MYSQL_PORT);
            try {
                // then source connects successfully
                assertEqualsEventually(() -> jet.getMap("results").size(), 4);
                assertEquals(RUNNING, job.getStatus());
            } finally {
                job.cancel();
                mysql.stop();
                assertJobStatusEventually(job, JobStatus.FAILED);
            }
        }
    }

    @Test
    public void when_networkDisconnectDuringSnapshotting_then_jetSourceIsStuckUntilReconnect() throws Exception {
        try (
                Network network = initNetwork();
                MySQLContainer<?> mysql = initMySql(network, null);
                ToxiproxyContainer toxiproxy = initToxiproxy(network);
        ) {
            ToxiproxyContainer.ContainerProxy proxy = initProxy(toxiproxy, mysql);
            Pipeline pipeline = initPipeline(proxy.getContainerIpAddress(), proxy.getProxyPort());
            // when job starts
            JetInstance jet = createJetMembers(2)[0];
            Job job = jet.newJob(pipeline);
            assertJobStatusEventually(job, RUNNING);

            // and snapshotting is ongoing (we have no exact way of identifying
            // the moment, but random sleep will catch it at least some of the time)
            TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(0, 500));

            // and connection is cut
            proxy.setConnectionCut(true);

            // and some time passes
            SECONDS.sleep(2 * MILLISECONDS.toSeconds(RECONNECT_INTERVAL_MS));

            // and connection recovers
            proxy.setConnectionCut(false);

            // then connector manages to reconnect and finish snapshot
            try {
                assertEqualsEventually(() -> jet.getMap("results").size(), 4);
            } finally {
                job.cancel();
                assertJobStatusEventually(job, JobStatus.FAILED);
            }
        }
    }

    @Test
    public void when_databaseShutdownDuringSnapshotting() throws Exception {
        MySQLContainer<?> mysql = initMySql(null, MYSQL_PORT);
        Pipeline pipeline = initPipeline(mysql.getContainerIpAddress(), MYSQL_PORT);
        try {
            // when job starts
            JetInstance jet = createJetMembers(2)[0];
            Job job = jet.newJob(pipeline);
            assertJobStatusEventually(job, RUNNING);

            // and snapshotting is ongoing (we have no exact way of identifying
            // the moment, but random sleep will catch it at least some of the time)
            TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(100, 500));

            // and DB is stopped
            stopContainer(mysql);

            boolean neverReconnect = reconnectBehaviour.getMaxAttempts() == 0;
            if (neverReconnect) {
                // then job fails
                assertThatThrownBy(job::join)
                        .hasRootCauseInstanceOf(JetException.class)
                        .hasStackTraceContaining("Connection to database lost");
            } else {
                // and DB is started anew
                mysql = initMySql(null, MYSQL_PORT);

                // then snapshotting finishes successfully
                try {
                    assertEqualsEventually(() -> jet.getMap("results").size(), 4);
                    assertEquals(RUNNING, job.getStatus());
                } finally {
                    job.cancel();
                    assertJobStatusEventually(job, JobStatus.FAILED);
                }
            }
        } finally {
            mysql.stop();
        }
    }

    @Test
    public void when_networkDisconnectDuringBinlogRead_then_connectorReconnectsInternally() throws Exception {
        try (
                Network network = initNetwork();
                MySQLContainer<?> mysql = initMySql(network, null);
                ToxiproxyContainer toxiproxy = initToxiproxy(network);
        ) {
            ToxiproxyContainer.ContainerProxy proxy = initProxy(toxiproxy, mysql);
            Pipeline pipeline = initPipeline(proxy.getContainerIpAddress(), proxy.getProxyPort());
            // when connector is up and transitions to binlog reading
            JetInstance jet = createJetMembers(2)[0];
            Job job = jet.newJob(pipeline);
            assertEqualsEventually(() -> jet.getMap("results").size(), 4);
            SECONDS.sleep(3);
            insertRecords(mysql, 1005);
            assertEqualsEventually(() -> jet.getMap("results").size(), 5);

            // and the connection is cut
            proxy.setConnectionCut(true);

            // and some new events get generated in the DB
            insertRecords(mysql, 1006, 1007);

            // and some time passes
            TimeUnit.MILLISECONDS.sleep(2 * RECONNECT_INTERVAL_MS);

            // and the connection is re-established
            proxy.setConnectionCut(false);

            // then the connector catches up
            try {
                assertEqualsEventually(() -> jet.getMap("results").size(), 7);
            } finally {
                job.cancel();
                assertJobStatusEventually(job, JobStatus.FAILED);
            }
        }
    }

    @Test
    public void when_databaseShutdownDuringBinlogReading() throws Exception {
        MySQLContainer<?> mysql = initMySql(null, MYSQL_PORT);
        Pipeline pipeline = initPipeline(mysql.getContainerIpAddress(), MYSQL_PORT);
        try {
            // when connector is up and transitions to binlog reading
            JetInstance jet = createJetMembers(2)[0];
            Job job = jet.newJob(pipeline);
            assertEqualsEventually(() -> jet.getMap("results").size(), 4);
            SECONDS.sleep(3);
            insertRecords(mysql, 1005);
            assertEqualsEventually(() -> jet.getMap("results").size(), 5);

            // and DB is stopped
            stopContainer(mysql);

            boolean neverReconnect = reconnectBehaviour.getMaxAttempts() == 0;
            if (neverReconnect) {
                // then job fails
                assertThatThrownBy(job::join)
                        .hasRootCauseInstanceOf(JetException.class)
                        .hasStackTraceContaining("Connection to database lost");
            } else {
                // and results are cleared
                jet.getMap("results").clear();
                assertEqualsEventually(() -> jet.getMap("results").size(), 0);

                // and DB is started anew
                mysql = initMySql(null, MYSQL_PORT);
                insertRecords(mysql, 1005, 1006, 1007);

                try {
                    if (resetStateOnReconnect) {
                        // then job keeps running, connector starts freshly, including snapshotting
                        assertEqualsEventually(() -> jet.getMap("results").size(), 7);
                        assertEquals(RUNNING, job.getStatus());
                    } else {
                        assertEqualsEventually(() -> jet.getMap("results").size(), 2);
                        assertEquals(RUNNING, job.getStatus());
                    }
                } finally {
                    job.cancel();
                    assertJobStatusEventually(job, JobStatus.FAILED);
                }
            }
        } finally {
            mysql.stop();
        }
    }

    private StreamSource<ChangeRecord> source(String host, int port) {
        return MySqlCdcSources.mysql("customers")
                .setDatabaseAddress(host)
                .setDatabasePort(port)
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1").setDatabaseWhitelist("inventory")
                .setTableWhitelist("inventory." + "customers")
                .setReconnectBehaviour(reconnectBehaviour)
                .setShouldStateBeResetOnReconnect(resetStateOnReconnect)
                .build();
    }

    private Pipeline initPipeline(String host, int port) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source(host, port))
                .withNativeTimestamps(0)
                .map(r -> entry(r.key().toMap().get("id"), r.value().toJson()))
                .writeTo(Sinks.map("results"));
        return pipeline;
    }

    private static Network initNetwork() {
        return Network.newNetwork();
    }

    private static MySQLContainer<?> initMySql(Network network, Integer fixedExposedPort) {
        MySQLContainer<?> mysql = new MySQLContainer<>("debezium/example-mysql:1.2")
                .withUsername("mysqluser")
                .withPassword("mysqlpw");
        if (fixedExposedPort != null) {
            Consumer<CreateContainerCmd> cmd = e -> e.withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(fixedExposedPort), new ExposedPort(fixedExposedPort)));
            mysql = mysql.withCreateContainerCmdModifier(cmd);
        }
        if (network != null) {
            mysql = mysql.withNetwork(network);
        }
        mysql.start();
        return mysql;
    }

    private static ToxiproxyContainer initToxiproxy(Network network) {
        ToxiproxyContainer toxiproxy = new ToxiproxyContainer().withNetwork(network);
        toxiproxy.start();
        return toxiproxy;
    }

    private static ToxiproxyContainer.ContainerProxy initProxy(ToxiproxyContainer toxiproxy, MySQLContainer<?> mysql) {
        return toxiproxy.getProxy(mysql, MYSQL_PORT);
    }

    private static void insertRecords(MySQLContainer<?> mysql, int... ids) throws SQLException {
        try (Connection connection = DriverManager.getConnection(mysql.withDatabaseName("inventory").getJdbcUrl(),
                mysql.getUsername(), mysql.getPassword())) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (int id : ids) {
                statement.addBatch("INSERT INTO customers VALUES (" + id + ", 'Jason', 'Bourne', " +
                        "'jason" + id + "@bourne.org')");
            }
            statement.executeBatch();
            connection.commit();
        }
    }

}