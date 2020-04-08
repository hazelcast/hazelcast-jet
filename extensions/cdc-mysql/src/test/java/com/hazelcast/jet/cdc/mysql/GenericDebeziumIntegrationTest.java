/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.mysql;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.cdc.ChangeEventElement;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.DebeziumCdcSources;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.mysql.data.Customer;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class GenericDebeziumIntegrationTest extends AbstractIntegrationTest {

    @Test
    public void mysql() throws Exception {
        MySQLContainer<?> container = new MySQLContainer<>("debezium/example-mysql")
                .withUsername("mysqluser")
                .withPassword("mysqlpw");

        try {
            container.start();

            // given
            String[] expectedEvents = {
                    "1001/0:INSERT:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                    "1002/0:INSERT:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                    "1003/0:INSERT:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                    "1004/0:INSERT:Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}",
                    "1004/1:UPDATE:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                    "1005/0:INSERT:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}",
                    "1005/1:DELETE:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
            };

            StreamSource<ChangeEvent> source = DebeziumCdcSources.debezium("mysql",
                    "io.debezium.connector.mysql.MySqlConnector")
                    .setCustomProperty("include.schema.changes", "false")
                    .setCustomProperty("database.hostname", container.getContainerIpAddress())
                    .setCustomProperty("database.port", Integer.toString(container.getMappedPort(MYSQL_PORT)))
                    .setCustomProperty("database.user", "debezium")
                    .setCustomProperty("database.password", "dbz")
                    .setCustomProperty("database.server.id", "184054")
                    .setCustomProperty("database.server.name", "dbserver1")
                    .setCustomProperty("database.whitelist", "inventory")
                    .setCustomProperty("table.whitelist", "inventory.customers")
                    .build();

            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(source)
                    .withNativeTimestamps(0)
                    .<ChangeEvent>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                    .groupingKey(event -> event.key().getInteger("id").orElse(0))
                    .mapStateful(
                            LongAccumulator::new,
                            (accumulator, customerId, event) -> {
                                long count = accumulator.get();
                                accumulator.add(1);
                                ChangeEventValue eventValue = event.value();
                                Operation operation = eventValue.operation();
                                ChangeEventElement mostRecentImage = DELETE.equals(operation) ?
                                        eventValue.before() : eventValue.after();
                                Customer customer = mostRecentImage.mapToObj(Customer.class);
                                return customerId + "/" + count + ":" + operation + ":" + customer;
                            })
                    .setLocalParallelism(1)
                    .writeTo(assertCollectedEventually(30, assertListFn(expectedEvents)));

            // when
            JetInstance jet = createJetMember();
            Job job = jet.newJob(pipeline);
            JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);

            HazelcastTestSupport.sleepAtLeastSeconds(10);
            // update a record
            try (Connection connection = DriverManager.getConnection(container.withDatabaseName("inventory").getJdbcUrl(),
                    container.getUsername(), container.getPassword())) {
                connection
                        .prepareStatement("UPDATE customers SET first_name='Anne Marie' WHERE id=1004")
                        .executeUpdate();
                connection
                        .prepareStatement("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')")
                        .executeUpdate();
                connection
                        .prepareStatement("DELETE FROM customers WHERE id=1005")
                        .executeUpdate();
            }

            // then
            try {
                job.join();
                fail("Job should have completed with an AssertionCompletedException, but completed normally");
            } catch (CompletionException e) {
                String errorMsg = e.getCause().getMessage();
                assertTrue("Job was expected to complete with " +
                                "AssertionCompletedException, but completed with: " + e.getCause(),
                        errorMsg.contains(AssertionCompletedException.class.getName()));
            }
        } finally {
            container.stop();
        }
    }
}
