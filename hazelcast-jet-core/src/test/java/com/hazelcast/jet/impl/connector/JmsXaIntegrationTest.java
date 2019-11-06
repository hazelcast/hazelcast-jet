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

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.jms.Connection;
import javax.jms.JMSContext;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class JmsXaIntegrationTest extends JetTestSupport {

    @ClassRule
    public static EmbeddedActiveMQResource resource = new EmbeddedActiveMQResource();

    private static int counter;

    @Test
    public void stressTest_forceful() throws Exception {
        stressTest(false);
    }

    @Test
    public void stressTest_graceful() throws Exception {
        stressTest(true);
    }

    private void stressTest(boolean graceful) throws Exception {
        JetInstance instance1 = createJetMember();
        createJetMember();

        final int MESSAGE_COUNT = 10_000;
        Pipeline p = Pipeline.create();
        IList<List<Long>> sinkList = instance1.getList("sinkList");
        String queueName = "queue-" + counter++;
        p.drawFrom(Sources.jmsQueueBuilder(JmsXaIntegrationTest::getConnectionFactory)
                .destinationName(queueName)
                .build(msg -> Long.parseLong(((TextMessage) msg).getText())))
         .withoutTimestamps()
         .peek()
         .mapStateful(() -> (List<Long>) new ArrayList<Long>(),
                 (list, item) -> {
                     assert list.size() < MESSAGE_COUNT : "list size exceeded. List=" + list + ", item=" + item;
                     list.add(item);
                     return list.size() == MESSAGE_COUNT ? list : null;
                 })
         .drainTo(Sinks.list(sinkList));

        Job job = instance1.newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50));
        assertJobStatusEventually(job, RUNNING);

        // start a producer that will produce MESSAGE_COUNT messages on the background to the queue, 1000 msgs/s
        Future producerFuture = spawn(() -> {
            try (
                    Connection connection = getConnectionFactory().createConnection();
                    Session session = connection.createSession(JMSContext.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(session.createQueue(queueName))
            ) {
                long startTime = System.nanoTime();
                for (int i = 0; i < MESSAGE_COUNT; i++) {
                    producer.send(session.createTextMessage(String.valueOf(i)));
                    Thread.sleep(Math.max(0,
                            Math.min(MESSAGE_COUNT, i) - NANOSECONDS.toMillis(System.nanoTime() - startTime)));
                }
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        });

        while (!producerFuture.isDone()) {
            Thread.sleep(ThreadLocalRandom.current().nextInt(200));
            ((JobProxy) job).restart(graceful);
            assertJobStatusEventually(job, RUNNING);
        }
        producerFuture.get(); // call for the side-effect of throwing if the producer failed

        // the list can contain the result multiple times, the sink isn't idempotent
        assertTrueEventually(() -> assertGreaterOrEquals("size", sinkList.size(), 1), 30);
        List<Long> result = sinkList.get(0);
        assertEquals(
                LongStream.range(0, MESSAGE_COUNT).mapToObj(Long::toString).collect(Collectors.joining("\n")),
                result.stream().sorted().map(Object::toString).collect(Collectors.joining("\n")));
    }

    // TODO [viliam] test upgrading from transacted to non-transacted and vice versa

    private static ActiveMQConnectionFactory getConnectionFactory() {
        return new ActiveMQXAConnectionFactory(resource.getVmURL());
    }
}
