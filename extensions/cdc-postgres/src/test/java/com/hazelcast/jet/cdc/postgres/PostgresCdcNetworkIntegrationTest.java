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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class PostgresCdcNetworkIntegrationTest extends AbstractCdcIntegrationTest {

    @Rule
    public Network network = Network.newNetwork();

    @Rule
    public PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("debezium/example-postgres:1.2")
            .withNetwork(network)
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres");

    @Rule
    public ToxiproxyContainer toxiproxy = new ToxiproxyContainer()
            .withNetwork(network);

    private ToxiproxyContainer.ContainerProxy proxy;

    @Before
    public void before() {
        proxy = toxiproxy.getProxy(postgres, POSTGRESQL_PORT);
    }

    @Test
    @Category(NightlyTest.class)
    public void when_networkDisconnectDuringBinlogRead_then_connectorReconnectsInternally() throws Exception {
        // given
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source(proxy.getContainerIpAddress(), proxy.getProxyPort()))
                .withNativeTimestamps(0)
                .writeTo(CdcSinks.map("results", r -> r.key().toMap().get("id"), r -> r.value().toJson()));

        // when
        JetInstance jet = createJetMembers(2)[0];
        Job job = jet.newJob(pipeline);

        //then
        assertEqualsEventually(() -> jet.getMap("results").size(), 4);

        //wait transition to WAL reading
        SECONDS.sleep(3);

        //cut the connection
        proxy.setConnectionCut(true);

        //generate events
        try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword())) {
            connection.setSchema("inventory");
            connection.createStatement()
                    .execute("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");
        }

        //wait a bit
        SECONDS.sleep(3);

        //re-establish the connection
        System.err.println("Re-enabling the connection ..."); //todo: remove
        proxy.setConnectionCut(false);

        //then
        try {
            assertEqualsEventually(() -> jet.getMap("results").size(), 5);
        } finally {
            job.cancel();
        }
    }

    @Test
    @Category(NightlyTest.class)
    @Ignore //only semi-automatic ...
    public void when_databaseShutdownDetectedDuringBinlogRead_then_connectorGetsRestarted() throws Exception {
        String host = "127.0.0.1";
        Integer port = POSTGRESQL_PORT;
        String user = "postgres";
        String password = "postgres";

        while (!canConnect(host, port, user, password)) {
            System.err.println("Start DB with following command: docker run -it --rm --name postgres -p 5432:5432 -e " +
                    "POSTGRES_DB=postgres -e POSTGRES_USER=" + user + " -e POSTGRES_PASSWORD=" +
                    password + " debezium/example-postgres:1.2");
            SECONDS.sleep(3);
        }

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source(host, port))
                .withNativeTimestamps(0)
                .writeTo(CdcSinks.map("results", r -> r.key().toMap().get("id"), r -> r.value().toJson()));

        JetInstance jet = createJetMembers(2)[0];
        Job job = jet.newJob(pipeline);

        assertEqualsEventually(() -> jet.getMap("results").size(), 4);

        while (canConnect(host, port, user, password)) {
            System.err.println("Kill DB with following command: docker container stop postgres");
            SECONDS.sleep(1);
        }

        while (!canConnect(host, port, user, password)) {
            System.err.println("Start DB with following command: docker run -it --rm --name postgres -p 5432:5432 -e " +
                    "POSTGRES_DB=postgres -e POSTGRES_USER=" + user + " -e POSTGRES_PASSWORD=" + password +
                    " debezium/example-postgres:1.2");
            SECONDS.sleep(3);
        }

        SECONDS.sleep(30); //todo: postgres recovery is problematic, if we insert data while disconnected, then it's lost

        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/postgres"
                , user, password)) {
            connection.setSchema("inventory");
            connection.createStatement().execute(
                    "INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");
        }

        //then
        try {
            assertEqualsEventually(() -> jet.getMap("results").size(), 5);
        } finally {
            job.cancel();
        }
    }

    private static boolean canConnect(String host, int port, String user, String password) {
        try {
            DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/postgres", user, password);
            return true;
        } catch (SQLException throwables) {
            return false;
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
                .setCustomProperty("status.update.interval.ms", "1000") //todo: always set for source, like MySQL keepalive
                .build();
    }

}
