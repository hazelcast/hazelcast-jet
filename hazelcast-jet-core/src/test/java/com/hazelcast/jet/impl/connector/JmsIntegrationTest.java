/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;

public class JmsIntegrationTest extends PipelineTestSupport {

    @ClassRule
    public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    private static final int MESSAGE_COUNT = 100;
    private static final DistributedFunction<Message, String> TEXT_MESSAGE_FN = m ->
            uncheckCall(((TextMessage) m)::getText);

    private String destinationName = randomString();
    private Job job;

    @After
    public void cleanup() {
        cancelJob();
    }

    @Test
    public void sourceQueue() {
        p.drawFrom(Sources.jmsQueue(() -> broker.createConnectionFactory(), destinationName))
         .map(TEXT_MESSAGE_FN)
         .drainTo(sink);

        startJob();

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);
    }

    @Test
    public void sourceTopic() {
        p.drawFrom(Sources.jmsTopic(() -> broker.createConnectionFactory(), destinationName))
         .map(TEXT_MESSAGE_FN)
         .drainTo(sink);

        startJob();
        sleepSeconds(1);

        List<Object> messages = sendMessages(false);
        assertEqualsEventually(sinkList::size, MESSAGE_COUNT);
        assertContainsAll(sinkList, messages);
    }

    @Test
    public void sinkQueue() throws JMSException {
        populateList();

        p.drawFrom(Sources.list(srcList.getName()))
         .drainTo(Sinks.jmsQueue(() -> broker.createConnectionFactory(), destinationName));

        List<Object> messages = consumeMessages(true);

        startJob();

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic() throws JMSException {
        populateList();

        p.drawFrom(Sources.list(srcList.getName()))
         .drainTo(Sinks.jmsTopic(() -> broker.createConnectionFactory(), destinationName));

        List<Object> messages = consumeMessages(false);
        sleepSeconds(1);

        startJob();

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sourceQueue_whenBuilder() {
        StreamSource<Message> source = Sources.jmsQueueBuilder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .build();

        p.drawFrom(source)
         .map(TEXT_MESSAGE_FN)
         .drainTo(sink);

        startJob();

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);
    }

    @Test
    public void sourceQueue_whenBuilder_withFunctions() {
        String queueName = destinationName;
        StreamSource<String> source = Sources.<String>jmsQueueBuilder(() -> broker.createConnectionFactory())
                .connectionFn(factory -> uncheckCall(factory::createConnection))
                .sessionFn(connection -> uncheckCall(() -> connection.createSession(false, AUTO_ACKNOWLEDGE)))
                .consumerFn(session -> uncheckCall(() -> session.createConsumer(session.createQueue(queueName))))
                .flushFn(noopConsumer())
                .build(TEXT_MESSAGE_FN);

        p.drawFrom(source).drainTo(sink);

        startJob();

        List<Object> messages = sendMessages(true);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);
    }

    @Test
    public void sourceTopic_whenBuilder() {
        StreamSource<String> source = Sources.<String>jmsTopicBuilder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .build(TEXT_MESSAGE_FN);

        p.drawFrom(source).drainTo(sink);

        startJob();
        sleepSeconds(1);

        List<Object> messages = sendMessages(false);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);
    }

    @Test
    public void sourceTopic_whenBuilder_withParameters() {
        StreamSource<String> source = Sources.<String>jmsTopicBuilder(() -> broker.createConnectionFactory())
                .connectionParams(null, null)
                .sessionParams(false, AUTO_ACKNOWLEDGE)
                .destinationName(destinationName)
                .build(TEXT_MESSAGE_FN);

        p.drawFrom(source).drainTo(sink);

        startJob();
        sleepSeconds(1);

        List<Object> messages = sendMessages(false);
        assertEqualsEventually(sinkList::size, messages.size());
        assertContainsAll(sinkList, messages);
    }

    @Test
    public void sinkQueue_whenBuilder() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsQueueBuilder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .build();

        p.drawFrom(Sources.<String>list(srcList.getName()))
         .drainTo(sink);

        List<Object> messages = consumeMessages(true);

        startJob();

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkQueue_whenBuilder_withFunctions() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsQueueBuilder(() -> broker.createConnectionFactory())
                .connectionFn(factory -> uncheckCall(factory::createConnection))
                .sessionFn(connection -> uncheckCall(() -> connection.createSession(false, AUTO_ACKNOWLEDGE)))
                .messageFn((session, item) -> uncheckCall(() -> session.createTextMessage(item)))
                .sendFn((producer, message) -> uncheckRun(() -> producer.send(message)))
                .flushFn(noopConsumer())
                .destinationName(destinationName)
                .build();

        p.drawFrom(Sources.<String>list(srcList.getName()))
         .drainTo(sink);

        List<Object> messages = consumeMessages(true);

        startJob();

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic_whenBuilder() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsTopicBuilder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .build();

        p.drawFrom(Sources.<String>list(srcList.getName()))
         .drainTo(sink);

        List<Object> messages = consumeMessages(false);
        sleepSeconds(1);

        startJob();

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    @Test
    public void sinkTopic_whenBuilder_withParameters() throws JMSException {
        populateList();

        Sink<String> sink = Sinks.<String>jmsTopicBuilder(() -> broker.createConnectionFactory())
                .connectionParams(null, null)
                .sessionParams(false, AUTO_ACKNOWLEDGE)
                .destinationName(destinationName)
                .build();

        p.drawFrom(Sources.<String>list(srcList.getName()))
         .drainTo(sink);

        List<Object> messages = consumeMessages(false);
        sleepSeconds(1);

        startJob();

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(srcList, messages);
    }

    private List<Object> consumeMessages(boolean isQueue) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = broker.createConnectionFactory();
        Connection connection = connectionFactory.createConnection();
        connection.start();

        List<Object> messages = synchronizedList(new ArrayList<>());
        spawn(() -> {
            try {
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
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        });
        return messages;
    }


    private List<Object> sendMessages(boolean isQueue) {
        return range(0, MESSAGE_COUNT)
                .mapToObj(i -> uncheckCall(() -> sendMessage(destinationName, isQueue)))
                .collect(toList());
    }

    private void populateList() {
        range(0, MESSAGE_COUNT).mapToObj(i -> randomString()).forEach(srcList::add);
    }

    private void startJob() {
        job = start();
        waitForJobStatus(JobStatus.RUNNING);
    }

    private void cancelJob() {
        job.cancel();
        waitForJobStatus(JobStatus.COMPLETED);
    }


    private void waitForJobStatus(JobStatus status) {
        while (job.getStatus() != status) {
            sleepMillis(1);
        }
    }

    private String sendMessage(String destinationName, boolean isQueue) throws Exception {
        String message = randomString();

        ActiveMQConnectionFactory connectionFactory = broker.createConnectionFactory();
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, AUTO_ACKNOWLEDGE);
        Destination destination = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        TextMessage textMessage = session.createTextMessage(message);
        producer.send(textMessage);
        session.close();
        connection.close();
        return message;
    }

}
