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
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.IntStream.range;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmsIntegrationTest extends SimpleTestInClusterSupport {

    @ClassRule
    public static EmbeddedActiveMQResource resource = new EmbeddedActiveMQResource();

    private static final int MESSAGE_COUNT = 100;
    private static final FunctionEx<Message, String> TEXT_MESSAGE_FN = m -> ((TextMessage) m).getText();

    private static int counter;
    private String destinationName = "dest" + counter++;
    private Job job;

    private Pipeline p = Pipeline.create();
    private IList<Object> srcList;
    private IList<Object> sinkList;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Before
    public void before() {
        srcList = instance().getList("src-" + counter++);
        sinkList = instance().getList("sink-" + counter++);
    }

    @Test
    public void sourceQueue() throws JMSException {
        p.readFrom(Sources.jmsQueue(() -> getConnectionFactory(false), destinationName))
         .withoutTimestamps()
         .map(TEXT_MESSAGE_FN)
         .writeTo(Sinks.list(sinkList));

        startJob(true);

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sourceTopic() throws JMSException {
        p.readFrom(Sources.jmsTopic(() -> getConnectionFactory(false), destinationName))
         .withoutTimestamps()
         .map(TEXT_MESSAGE_FN)
         .writeTo(Sinks.list(sinkList));

        startJob(true);
        sleepSeconds(1);

        List<Object> messages = sendMessages(false);
        assertEqualsEventually(sinkList::size, MESSAGE_COUNT);
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sinkQueue() throws JMSException {
        populateList();

        p.readFrom(Sources.list(srcList.getName()))
         .writeTo(Sinks.jmsQueue(() -> getConnectionFactory(false), destinationName));

        List<Object> messages = consumeMessages(true);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic() throws JMSException {
        populateList();

        p.readFrom(Sources.list(srcList.getName()))
         .writeTo(Sinks.jmsTopic(() -> getConnectionFactory(false), destinationName));

        List<Object> messages = consumeMessages(false);
        sleepSeconds(1);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sourceQueue_whenBuilder() throws JMSException {
        StreamSource<Message> source = Sources.jmsQueueBuilder(() -> getConnectionFactory(false))
                                              .destinationName(destinationName)
                                              .build();

        p.readFrom(source)
         .withoutTimestamps()
         .map(TEXT_MESSAGE_FN)
         .writeTo(Sinks.list(sinkList));

        startJob(true);

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sourceQueue_whenBuilder_withFunctions() throws JMSException {
        String queueName = destinationName;
        StreamSource<String> source = Sources.jmsQueueBuilder(() -> getConnectionFactory(false))
                .connectionFn(ConnectionFactory::createConnection)
                .consumerFn(session -> session.createConsumer(session.createQueue(queueName)))
                .build(TEXT_MESSAGE_FN);

        p.readFrom(source).withoutTimestamps().writeTo(Sinks.list(sinkList));

        startJob(true);

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sourceTopic_withNativeTimestamps() throws Exception {
        p.readFrom(Sources.jmsTopic(() -> getConnectionFactory(false), destinationName))
         .withNativeTimestamps(0)
         .map(Message::getJMSTimestamp)
         .window(tumbling(1))
         .aggregate(counting())
         .writeTo(Sinks.list(sinkList));

        startJob(true);
        sendMessages(false);
        // sleep some time and emit a flushing message, that won't make it to the output, because
        // the messages with the highest timestamp are not emitted
        sleepMillis(500);
        sendMessages(false, 1);

        assertTrueEventually(() -> {
            long countSum = sinkList.stream().mapToLong(o -> ((WindowResult<Long>) o).result()).sum();
            assertEquals(MESSAGE_COUNT, countSum);

            // There's no way to see the JetEvent's timestamp by the user code. In order to check
            // the native timestamp, we aggregate the events into tumbling(1) windows and check
            // the timestamps of the windows: we assert that it is around the current time.
            long avgTime = (long) sinkList.stream().mapToLong(o -> ((WindowResult<Long>) o).end())
                                          .average().orElse(0);
            long tenMinutes = MINUTES.toMillis(1);
            long now = System.currentTimeMillis();
            assertTrue("Time too much off: " + Instant.ofEpochMilli(avgTime).atZone(ZoneId.systemDefault()),
                    avgTime > now - tenMinutes && avgTime < now + tenMinutes);
        }, 10);

        cancelJob();
    }

    @Test
    public void sourceTopic_whenBuilder() throws JMSException {
        StreamSource<String> source = Sources.jmsTopicBuilder(() -> getConnectionFactory(false))
                .destinationName(destinationName)
                .build(TEXT_MESSAGE_FN);

        p.readFrom(source).withoutTimestamps().writeTo(Sinks.list(sinkList));

        startJob(true);
        sleepSeconds(1);

        List<Object> messages = sendMessages(false);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);

        cancelJob();
    }

    @Test
    public void sinkQueue_whenBuilder() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsQueueBuilder(() -> getConnectionFactory(false))
                .destinationName(destinationName)
                .build();

        p.readFrom(Sources.<String>list(srcList.getName()))
         .writeTo(sink);

        List<Object> messages = consumeMessages(true);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkQueue_whenBuilder_withFunctions() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsQueueBuilder(() -> getConnectionFactory(false))
                .connectionFn(ConnectionFactory::createConnection)
                .sessionFn(connection -> connection.createSession(false, AUTO_ACKNOWLEDGE))
                .messageFn(Session::createTextMessage)
                .sendFn(MessageProducer::send)
                .flushFn(ConsumerEx.noop())
                .destinationName(destinationName)
                .build();

        p.readFrom(Sources.<String>list(srcList.getName()))
         .writeTo(sink);

        List<Object> messages = consumeMessages(true);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic_whenBuilder() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsTopicBuilder(() -> getConnectionFactory(false))
                .destinationName(destinationName)
                .build();

        p.readFrom(Sources.<String>list(srcList.getName()))
         .writeTo(sink);

        List<Object> messages = consumeMessages(false);
        sleepSeconds(1);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic_whenBuilder_withParameters() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsTopicBuilder(() -> getConnectionFactory(false))
                .connectionParams(null, null)
                .sessionParams(false, AUTO_ACKNOWLEDGE)
                .destinationName(destinationName)
                .build();

        p.readFrom(Sources.<String>list(srcList.getName()))
         .writeTo(sink);

        List<Object> messages = consumeMessages(false);
        sleepSeconds(1);

        startJob(false);

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

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

        final int MESSAGE_COUNT = 7_000;
        Pipeline p = Pipeline.create();
        IList<List<Long>> sinkList = instance1.getList("sinkList");
        String queueName = "queue-" + counter++;
        p.readFrom(Sources.jmsQueueBuilder(() -> getConnectionFactory(true))
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
         .writeTo(Sinks.list(sinkList));

        Job job = instance1.newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50));
        assertJobStatusEventually(job, RUNNING);

        // start a producer that will produce MESSAGE_COUNT messages on the background to the queue, 1000 msgs/s
        Future producerFuture = spawn(() -> {
            try (
                    Connection connection = getConnectionFactory(false).createConnection();
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

    private List<Object> consumeMessages(boolean isQueue) throws JMSException {
        ConnectionFactory connectionFactory = getConnectionFactory(false);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        List<Object> messages = synchronizedList(new ArrayList<>());
        spawn(() -> uncheckRun(() -> {
            Session session = connection.createSession(false, AUTO_ACKNOWLEDGE);
            Destination dest = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
            MessageConsumer consumer = session.createConsumer(dest);
            int count = 0;
            while (count < MESSAGE_COUNT) {
                messages.add(((TextMessage) consumer.receive()).getText());
                count++;
            }
            consumer.close();
            session.close();
            connection.close();
        }));
        return messages;
    }

    private List<Object> sendMessages(boolean isQueue) throws JMSException {
        return sendMessages(isQueue, MESSAGE_COUNT);
    }

    private void populateList() {
        range(0, MESSAGE_COUNT).mapToObj(i -> randomString()).forEach(srcList::add);
    }

    private void startJob(boolean waitForRunning) {
        job = instance().newJob(p);
        // batch jobs can be completed before we observe RUNNING status
        if (waitForRunning) {
            assertJobStatusEventually(job, JobStatus.RUNNING, 10);
        }
    }

    private void cancelJob() {
        job.cancel();
        assertJobStatusEventually(job, JobStatus.FAILED, 10);
    }

    private List<Object> sendMessages(boolean isQueue, int count) throws JMSException {
        try (
                Connection conn = getConnectionFactory(false).createConnection();
                Session session = conn.createSession(false, AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(
                        isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName));
        ) {
            List<Object> res = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String message = randomString();
                producer.send(session.createTextMessage(message));
                res.add(message);
            }
            return res;
        }
    }

    private static ConnectionFactory getConnectionFactory(boolean xa) {
        return xa
                ? new ActiveMQXAConnectionFactory(resource.getVmURL())
                : new ActiveMQConnectionFactory(resource.getVmURL());
    }
}
