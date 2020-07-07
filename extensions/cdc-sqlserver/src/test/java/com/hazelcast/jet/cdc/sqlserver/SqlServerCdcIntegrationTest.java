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

package com.hazelcast.jet.cdc.sqlserver;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MSSQLServerContainer;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

public class SqlServerCdcIntegrationTest extends AbstractCdcIntegrationTest {

    @Rule
    public MSSQLServerContainer<?> mssql = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server")
            .withEnv("MSSQL_AGENT_ENABLED", "true")
            .withClasspathResourceMapping("setup.sql",
                    "/tmp/setup.sql", BindMode.READ_ONLY)
            .withClasspathResourceMapping("cdc.sql",
                    "/tmp/cdc.sql", BindMode.READ_ONLY);

    @Test
    public void customers() throws Exception {
        execInContainer("setup.sql");
        HazelcastTestSupport.assertTrueEventually(() -> {
            Container.ExecResult result = execInContainer("cdc.sql");
            HazelcastTestSupport.assertContains(result.getStdout(), "already");
        });

        // given
        List<String> expectedRecords = Arrays.asList(
                "1001/0:SYNC:" + new Customer(1001, "Sally", "Thomas", "sally.thomas@acme.com"),
                "1002/0:SYNC:" + new Customer(1002, "George", "Bailey", "gbailey@foobar.com"),
                "1003/0:SYNC:" + new Customer(1003, "Edward", "Walker", "ed@walker.com"),
                "1004/0:SYNC:" + new Customer(1004, "Anne", "Kretchmar", "annek@noanswer.org"),
                "1004/1:UPDATE:" + new Customer(1004, "Anne Marie", "Kretchmar", "annek@noanswer.org"),
                "1005/0:INSERT:" + new Customer(1005, "Jason", "Bourne", "jason@bourne.org"),
                "1005/1:DELETE:" + new Customer(1005, "Jason", "Bourne", "jason@bourne.org")
        );

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source("customers"))
                .withNativeTimestamps(0)
                .<ChangeRecord>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                .groupingKey(record -> (Integer) record.key().toMap().get("id"))
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, customerId, record) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            Operation operation = record.operation();
                            RecordPart value = record.value();
                            Customer customer = value.toObject(Customer.class);
                            return entry(customerId + "/" + count, operation + ":" + customer);
                        })
                .setLocalParallelism(1)
                .writeTo(Sinks.map("results"));

        // when
        JetInstance jet = createJetMembers(2)[0];
        Job job = jet.newJob(pipeline);

        //then
        assertEqualsEventually(() -> jet.getMap("results").size(), 4);

        //when
        try (Connection connection = DriverManager.getConnection(mssql.getJdbcUrl() + ";databaseName=MyDB",
                mssql.getUsername(), mssql.getPassword())) {
            connection.setSchema("inventory");
            Statement statement = connection.createStatement();
            statement.addBatch("UPDATE MyDB.inventory.customers SET first_name = 'Anne Marie' WHERE id = 1004");
            statement.addBatch("INSERT INTO MyDB.inventory.customers (first_name, last_name, email) VALUES " +
                    "('Jason', 'Bourne', 'jason@bourne.org')");
            statement.addBatch("DELETE FROM MyDB.inventory.customers WHERE last_name='Bourne'");
            statement.executeBatch();
        }

        //then
        try {
            assertEqualsEventually(() -> mapResultsToSortedList(jet.getMap("results")), expectedRecords);
        } finally {
            job.cancel();
        }
    }

    @Nonnull
    private StreamSource<ChangeRecord> source(String tableName) {
        return SqlServerCdcSources.sqlserver(tableName)
                .setDatabaseAddress(mssql.getContainerIpAddress())
                .setDatabasePort(mssql.getMappedPort(MS_SQL_SERVER_PORT))
                .setDatabaseUser(mssql.getUsername())
                .setDatabasePassword(mssql.getPassword())
                .setDatabaseName("MyDB")
                .setClusterName("fulfillment")
                .setTableWhitelist("inventory." + tableName)
                .build();
    }

    private Container.ExecResult execInContainer(String script) throws Exception {
        return mssql.execInContainer("/opt/mssql-tools/bin/sqlcmd", "-S", "localhost", "-U", mssql.getUsername(),
                "-P", mssql.getPassword(), "-d", "master", "-i", "/tmp/" + script);
    }

    private static class Customer implements Serializable {

        @JsonProperty("id")
        public int id;

        @JsonProperty("first_name")
        public String firstName;

        @JsonProperty("last_name")
        public String lastName;

        @JsonProperty("email")
        public String email;

        Customer() {
        }

        Customer(int id, String firstName, String lastName, String email) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
        }

        int getId() {
            return id;
        }

        String getFirstName() {
            return firstName;
        }

        String getLastName() {
            return lastName;
        }

        String getEmail() {
            return email;
        }

        @Override
        public int hashCode() {
            return Objects.hash(email, firstName, id, lastName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Customer other = (Customer) obj;
            return id == other.id
                    && Objects.equals(firstName, other.firstName)
                    && Objects.equals(lastName, other.lastName)
                    && Objects.equals(email, other.email);
        }

        @Override
        public String toString() {
            return "Customer {id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", email=" + email + '}';
        }
    }

}
