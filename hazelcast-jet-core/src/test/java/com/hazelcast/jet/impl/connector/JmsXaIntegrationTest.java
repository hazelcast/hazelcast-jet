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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.map.IMap;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.JMSContext;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;

public class JmsXaIntegrationTest extends JetTestSupport {

    @ClassRule
    public static EmbeddedActiveMQResource resource = new EmbeddedActiveMQResource();

    @Test
    public void test() throws Exception {
        JetInstance instance1 = createJetMember();
        JetInstance instance2 = createJetMember();

        final int MESSAGE_COUNT = 3_000; // TODO [viliam] change to more
        Pipeline p = Pipeline.create();
        IMap<Long, List<Long>> sinkMap = instance1.getMap("sinkMap");
        String queueName = "queue-" + randomName();
        p.drawFrom(
                Sources.jmsQueueBuilder(JmsXaIntegrationTest::getConnectionFactory)
                       .destinationName(queueName)
                       .build())
         .withTimestamps(msg -> Long.parseLong(((TextMessage) msg).getText()), 0)
         .map(msg -> Long.parseLong(((TextMessage) msg).getText()))
         .window(WindowDefinition.tumbling(MESSAGE_COUNT))
         .aggregate(AggregateOperations.toList())
         .peek()
         .map(winResult -> entry(winResult.start(), winResult.result()))
         .drainTo(Sinks.map(sinkMap));

        Job job = instance1.newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50));

        assertJobStatusEventually(job, RUNNING);

        // start a producer that will produce MESSAGE_COUNT messages on the background, 1000 msgs/s
        Future producerFuture = spawn(() -> {
            try (
                    Connection connection = getConnectionFactory().createConnection();
                    Session session = connection.createSession(JMSContext.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(session.createQueue(queueName))
            ) {
                long startTime = System.nanoTime();
                // produce 2*MESSAGE_COUNT items. This will create 2 full tumbling windows, but the 2nd
                // window won't close because there won't be a 3rd window to cause the watermark to go beyond
                // the 2nd window. And we do the full 2nd window in order to be likely that all processors will
                // have item in it so that the WM can be coalesced and emitted.
                for (int i = 0; i < MESSAGE_COUNT * 2; i++) {
                    producer.send(session.createTextMessage(String.valueOf(i)));
                    Thread.sleep(Math.max(0,
                            Math.min(MESSAGE_COUNT, i) - NANOSECONDS.toMillis(System.nanoTime() - startTime)));
                }
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        });

        while (!producerFuture.isDone()) {
            Thread.sleep(100 + ThreadLocalRandom.current().nextInt(400));
            ((JobProxy) job).restart(false);
            assertJobStatusEventually(job, RUNNING);
        }
        producerFuture.get(); // call for the side-effect of throwing if the producer failed

        assertEquals(1, sinkMap.size());
        List<Long> result = sinkMap.get(0L);
        assertEquals(
                LongStream.range(0, MESSAGE_COUNT).mapToObj(Long::toString).collect(Collectors.joining("\n")),
                result.stream().sorted().map(Object::toString).collect(Collectors.joining("\n")));
    }

    private static ActiveMQConnectionFactory getConnectionFactory() {
        return new ActiveMQXAConnectionFactory(resource.getVmURL());
    }
}
