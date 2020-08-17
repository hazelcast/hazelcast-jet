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

package com.hazelcast.jet.cdc.postgres;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.impl.CdcSource.ReconnectBehaviour;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.cdc.impl.CdcSource.ReconnectBehaviour.CLEAR_STATE_AND_RECONNECT;
import static com.hazelcast.jet.cdc.impl.CdcSource.ReconnectBehaviour.FAIL;
import static com.hazelcast.jet.cdc.impl.CdcSource.ReconnectBehaviour.RECONNECT;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class PostgresCdcNetworkIntegrationTest extends AbstractCdcIntegrationTest {

    private static final long RECONNECT_INTERVAL_MS = SECONDS.toMillis(1);

    @Parameter
    public ReconnectBehaviour reconnectBehaviour;

    @Parameterized.Parameters(name = "{index}: behaviour={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(FAIL, RECONNECT, CLEAR_STATE_AND_RECONNECT);
    }

    @Test
    public void when_noDatabaseToConnectTo() {
        Pipeline pipeline = initPipeline("localhost", POSTGRESQL_PORT);

        // when job starts
        JetInstance jet = createJetMembers(2)[0];
        Job job = jet.newJob(pipeline);
        // then
        if (FAIL.equals(reconnectBehaviour)) {
            // then job fails
            assertThatThrownBy(job::join)
                    .hasRootCauseInstanceOf(JetException.class)
                    .hasStackTraceContaining("Connecting to database failed");
            assertTrue(jet.getMap("results").isEmpty());
        } else {
            // then can't connect to DB
            assertJobStatusEventually(job, RUNNING);
            assertTrueAllTheTime(() -> assertTrue(jet.getMap("results").isEmpty()),
                    MILLISECONDS.toSeconds(2 * RECONNECT_INTERVAL_MS));

            // when DB starts
            PostgreSQLContainer<?> postgres = initPostgres(null, POSTGRESQL_PORT);
            try {
                // then source connects successfully
                assertEqualsEventually(() -> jet.getMap("results").size(), 4);
                assertEquals(RUNNING, job.getStatus());
            } finally {
                job.cancel();
                postgres.stop();
            }
        }
    }

    @Test
    public void when_shortNetworkDisconnectDuringSnapshotting_then_connectorDoesNotNoticeAnything() throws Exception {
        try (
                Network network = initNetwork();
                PostgreSQLContainer<?> postgres = initPostgres(network, null);
                ToxiproxyContainer toxiproxy = initToxiproxy(network);
        ) {
            ToxiproxyContainer.ContainerProxy proxy = initProxy(toxiproxy, postgres);
            Pipeline pipeline = initPipeline(proxy.getContainerIpAddress(), proxy.getProxyPort());
            // when job starts
            JetInstance jet = createJetMembers(2)[0];
            Job job = jet.newJob(pipeline);
            assertJobStatusEventually(job, RUNNING);

            // and snapshotting is ongoing (we have no exact way of identifying
            // the moment, but random sleep will catch it at least some of the time)
            MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(0, 500));

            // and connection is cut
            proxy.setConnectionCut(true);

            // and some time passes
            MILLISECONDS.sleep(2 * RECONNECT_INTERVAL_MS);
            //it takes the bloody thing 150 seconds to notice the connection being down, so it won't notice this...

            // and connection recovers
            proxy.setConnectionCut(false);

            // then connector manages to reconnect and finish snapshot
            try {
                assertEqualsEventually(() -> jet.getMap("results").size(), 4);
            } finally {
                job.cancel();
            }
        }
    }

    @Test
    public void when_databaseShutdownOrLongDisconnectDuringSnapshotting() throws Exception {
        PostgreSQLContainer<?> postgres = initPostgres(null, POSTGRESQL_PORT);
        Pipeline pipeline = initPipeline(postgres.getContainerIpAddress(), POSTGRESQL_PORT);
        try {
            // when job starts
            JetInstance jet = createJetMembers(2)[0];
            Job job = jet.newJob(pipeline);
            assertJobStatusEventually(job, RUNNING);

            // and snapshotting is ongoing (we have no exact way of identifying
            // the moment, but random sleep will catch it at least some of the time)
            MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(100, 500));

            // and DB is stopped
            stopContainer(postgres);

            // then
            if (FAIL.equals(reconnectBehaviour)) {
                // then job fails
                assertThatThrownBy(job::join)
                        .hasRootCauseInstanceOf(JetException.class)
                        .hasStackTraceContaining("Connection to database lost");
            } else {
                // and DB is started anew
                postgres = initPostgres(null, POSTGRESQL_PORT);

                // then snapshotting finishes successfully
                try {
                    assertEqualsEventually(() -> jet.getMap("results").size(), 4);
                    assertEquals(RUNNING, job.getStatus());
                } finally {
                    job.cancel();
                }
            }
        } finally {
            postgres.stop();
        }
    }

    @Test
    public void when_shortConnectionLossDuringBinlogReading_then_connectorDoesNotNoticeAnything() throws Exception {
        try (
                Network network = initNetwork();
                PostgreSQLContainer<?> postgres = initPostgres(network, null);
                ToxiproxyContainer toxiproxy = initToxiproxy(network);
        ) {
            ToxiproxyContainer.ContainerProxy proxy = initProxy(toxiproxy, postgres);
            Pipeline pipeline = initPipeline(proxy.getContainerIpAddress(), proxy.getProxyPort());
            // when connector is up and transitions to binlog reading
            JetInstance jet = createJetMembers(2)[0];
            Job job = jet.newJob(pipeline);
            assertEqualsEventually(() -> jet.getMap("results").size(), 4);
            SECONDS.sleep(3);
            insertRecords(postgres, 1005);
            assertEqualsEventually(() -> jet.getMap("results").size(), 5);

            // and the connection is cut
            proxy.setConnectionCut(true);

            // and some new events get generated in the DB
            insertRecords(postgres, 1006, 1007);

            // and some time passes
            MILLISECONDS.sleep(5 * RECONNECT_INTERVAL_MS);

            // and the connection is re-established
            proxy.setConnectionCut(false);

            // then
            try {
                // then job keeps running, connector starts freshly, including snapshotting
                assertEqualsEventually(() -> jet.getMap("results").size(), 7);
                assertEquals(RUNNING, job.getStatus());
            } finally {
                job.cancel();
            }
        }
    }

    @Test
    public void when_databaseShutdownOrLongDisconnectDuringBinlogReading() throws Exception {
        if (RECONNECT.equals(reconnectBehaviour)) {
            return; //doesn't make sense to test this mode with this scenario
        }

        PostgreSQLContainer<?> postgres = initPostgres(null, POSTGRESQL_PORT);
        Pipeline pipeline = initPipeline(postgres.getContainerIpAddress(), POSTGRESQL_PORT);
        try {
            // when connector is up and transitions to binlog reading
            JetInstance jet = createJetMembers(2)[0];
            Job job = jet.newJob(pipeline);
            assertEqualsEventually(() -> jet.getMap("results").size(), 4);
            SECONDS.sleep(3);
            insertRecords(postgres, 1005);
            assertEqualsEventually(() -> jet.getMap("results").size(), 5);

            // and DB is stopped
            stopContainer(postgres);

            if (FAIL.equals(reconnectBehaviour)) {
                // then job fails
                assertThatThrownBy(job::join)
                        .hasRootCauseInstanceOf(JetException.class)
                        .hasStackTraceContaining("Connection to database lost");
            } else {
                // and results are cleared
                jet.getMap("results").clear();
                assertEqualsEventually(() -> jet.getMap("results").size(), 0);

                // and DB is started anew
                postgres = initPostgres(null, POSTGRESQL_PORT);
                insertRecords(postgres, 1005);

                // and some time passes
                SECONDS.sleep(3);
                insertRecords(postgres, 1006, 1007);

                try {
                    // then job keeps running, connector starts freshly, including snapshotting
                    assertEqualsEventually(() -> jet.getMap("results").size(), 7);
                    assertEquals(RUNNING, job.getStatus());
                } finally {
                    job.cancel();
                }
            }
        } finally {
            postgres.stop();
        }
    }

    @Nonnull
    private StreamSource<ChangeRecord> source(String host, int port) {
        return PostgresCdcSources.postgres("customers")
                .setDatabaseAddress(host)
                .setDatabasePort(port)
                .setDatabaseUser("postgres")
                .setDatabasePassword("postgres")
                .setDatabaseName("postgres")
                .setTableWhitelist("inventory.customers")
                .setReconnectIntervalMs(1000)
                .setReconnectBehaviour(reconnectBehaviour.name())
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

    private static PostgreSQLContainer<?> initPostgres(Network network, Integer fixedExposedPort) {
        PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("debezium/example-postgres:1.2")
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres");
        if (fixedExposedPort != null) {
            Consumer<CreateContainerCmd> cmd = e -> e.withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(fixedExposedPort), new ExposedPort(fixedExposedPort)));
            postgres = postgres.withCreateContainerCmdModifier(cmd);
        }
        if (network != null) {
            postgres = postgres.withNetwork(network);
        }
        postgres.start();
        waitUntilUp(postgres);
        return postgres;
    }

    private static ToxiproxyContainer initToxiproxy(Network network) {
        ToxiproxyContainer toxiproxy = new ToxiproxyContainer().withNetwork(network);
        toxiproxy.start();
        return toxiproxy;
    }

    private static ToxiproxyContainer.ContainerProxy initProxy(
            ToxiproxyContainer toxiproxy, PostgreSQLContainer<?> postgres) {
        return toxiproxy.getProxy(postgres, POSTGRESQL_PORT);
    }

    private static void waitUntilUp(PostgreSQLContainer<?> postgres) {
        for (; ; ) {
            try {
                try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                        postgres.getPassword())) {
                    connection.setSchema("inventory");
                    boolean successfull = connection.createStatement().execute("SELECT 1");
                    if (successfull) {
                        return;
                    }
                }
            } catch (SQLException throwables) {
                // repeat
            }
            System.out.println("Waiting for the database to come up...");
        }
    }

    private static void insertRecords(PostgreSQLContainer<?> postgres, int... ids) throws SQLException {
        try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword())) {
            connection.setSchema("inventory");
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
