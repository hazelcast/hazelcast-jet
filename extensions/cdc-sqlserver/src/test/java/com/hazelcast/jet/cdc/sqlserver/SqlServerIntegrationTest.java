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

package com.hazelcast.jet.cdc.sqlserver;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.cdc.ChangeEventElement;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.sqlserver.data.Customer;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MSSQLServerContainer;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.core.test.JetAssert.fail;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

public class SqlServerIntegrationTest extends AbstractIntegrationTest {

    @Rule
    public MSSQLServerContainer mssql = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server")
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

        String[] expectedEvents = {
                "1001/0:SYNC:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                "1002/0:SYNC:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                "1003/0:SYNC:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                "1004/0:SYNC:Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}",
                "1004/1:UPDATE:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                "1005/0:INSERT:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}",
                "1005/1:DELETE:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
        };

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source("customers"))
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
        // update record
        try (Connection connection = DriverManager.getConnection(mssql.getJdbcUrl() + ";databaseName=MyDB",
                mssql.getUsername(), mssql.getPassword())) {
            connection.setSchema("inventory");
            connection
                    .prepareStatement("UPDATE MyDB.inventory.customers SET first_name = 'Anne Marie' WHERE id = 1004")
                    .executeUpdate();
            connection
                    .prepareStatement("INSERT INTO MyDB.inventory.customers (first_name, last_name, email) " +
                            "VALUES ('Jason', 'Bourne', 'jason@bourne.org')")
                    .executeUpdate();
            connection
                    .prepareStatement("DELETE FROM MyDB.inventory.customers WHERE last_name='Bourne'")
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
    }

    @Nonnull
    private StreamSource<ChangeEvent> source(String tableName) {
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
}
