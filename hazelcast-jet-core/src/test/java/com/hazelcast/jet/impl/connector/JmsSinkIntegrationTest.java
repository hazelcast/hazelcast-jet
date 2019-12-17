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

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.jms.Session.DUPS_OK_ACKNOWLEDGE;
import static org.junit.Assert.assertEquals;

public class JmsSinkIntegrationTest extends SimpleTestInClusterSupport {
    @ClassRule
    public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Test
    public void test_transactional_withRestarts_graceful() throws Exception {
        test_transactional_withRestarts(true);
    }

    @Test
    public void test_transactional_withRestarts_forceful() throws Exception {
        test_transactional_withRestarts(false);
    }

    private void test_transactional_withRestarts(boolean graceful) throws Exception {
        int numItems = 1000;
        Pipeline p = Pipeline.create();
        String destinationName = randomString();
        p.readFrom(SourceBuilder.stream("src", procCtx -> new int[1])
                                .fillBufferFn((ctx, buf) -> {
                                    if (ctx[0] < numItems) {
                                        buf.add(ctx[0]++);
                                        sleepMillis(10);
                                    }
                                })
                                .createSnapshotFn(ctx -> ctx[0])
                                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                                .build())
         .withoutTimestamps()
         .map(String::valueOf)
         .writeTo(Sinks.jmsQueue(destinationName, () -> new ActiveMQXAConnectionFactory(broker.getVmURL())));

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        JobProxy job = (JobProxy) instance().newJob(p, config);

        try (
                Connection connection = broker.createConnectionFactory().createConnection();
                Session session = connection.createSession(false, DUPS_OK_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(session.createQueue(destinationName))
        ) {
            connection.start();
            long endTime = System.nanoTime() + SECONDS.toNanos(120);
            Set<Integer> actualSinkContents = new HashSet<>();

            int actualCount = 0;
            Set<Integer> expected = IntStream.range(0, numItems).boxed().collect(Collectors.toSet());
            // We'll restart once, then restart again after a short sleep (possibly during initialization), then restart
            // again and then assert some output so that the test isn't constantly restarting without any progress
            for (;;) {
                assertJobStatusEventually(job, RUNNING);
                job.restart(graceful);
                assertJobStatusEventually(job, RUNNING);
                sleepMillis(ThreadLocalRandom.current().nextInt(400));
                job.restart(graceful);
                try {
                    Message msg;
                    for (int countThisRound = 0; countThisRound < 100 && (msg = consumer.receive(5000)) != null; ) {
                        actualSinkContents.add(Integer.valueOf(((TextMessage) msg).getText()));
                        actualCount++;
                        countThisRound++;
                    }

                    logger.info("number of committed items in the sink so far: " + actualCount);
                    assertEquals(expected, actualSinkContents);
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
}
