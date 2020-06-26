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

import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public abstract class AbstractPostgresCdcIntegrationTest extends AbstractCdcIntegrationTest {

    @Rule
    public PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("debezium/example-postgres:1.2")
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres");

    @Before
    public void before() throws Exception {
        createReplicationSlot();
    }

    private void createReplicationSlot() throws Exception {
        String slot = "debezium";
        String plugin = "decoderbufs";

        try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword())) {
            logger.info(String.format("Creating replication slot %s for decoder plugin %s ...", slot, plugin));
            connection.createStatement().executeQuery("SELECT pg_create_logical_replication_slot('" + slot + "', '" + plugin + "');");
            logger.info(String.format("Replication slot %s created", slot));

            ResultSet rs;
            for (;;) {
                logger.info(String.format("Waiting for replication slot %s to be available...", slot));
                rs = connection.createStatement().executeQuery("SELECT slot_name FROM pg_replication_slots;");

                if (rs.next() && rs.getString("slot_name").equals(slot)) {
                    logger.info(String.format("Replication slot %s is available", slot));
                    return;
                }

                TimeUnit.MILLISECONDS.sleep(250);
            }
        }
    }

    protected PostgresCdcSources.Builder sourceBuilder(String name) {
        return PostgresCdcSources.postgres(name)
                .setDatabaseAddress(postgres.getContainerIpAddress())
                .setDatabasePort(postgres.getMappedPort(POSTGRESQL_PORT))
                .setDatabaseUser("postgres")
                .setDatabasePassword("postgres")
                .setDatabaseName("postgres");
    }

    protected void createSchema(String schema) throws SQLException {
        try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword())) {
            connection.createStatement().execute("CREATE SCHEMA " + schema);
        }
    }

}
