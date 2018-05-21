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

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.JmsSinkBuilder;
import com.hazelcast.jet.pipeline.JmsSourceBuilder;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

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

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

@RunWith(HazelcastParallelClassRunner.class)
public class JmsIntegrationTest extends JetTestSupport {

    @ClassRule
    public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    private static final int MESSAGE_COUNT = 100;
    private static final DistributedFunction<Message, String> TEXT_MESSAGE_FN = m ->
            uncheckCall(((TextMessage) m)::getText);

    private JetInstance instance;
    private String destinationName = randomString();
    private String listName = randomString();

    @Before
    public void setupInstance() {
        instance = createJetMember();
        createJetMember();
    }

    @Test
    public void sourceQueue() {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.jmsQueue(() -> broker.createConnectionFactory(), destinationName))
                .map(TEXT_MESSAGE_FN)
                .drainTo(Sinks.list(listName));

        Job job = instance.newJob(pipeline, jobConfig());

        List<String> messages = sendMessages(true);

        IListJet<String> list = instance.getList(listName);
        assertEqualsEventually(list::size, messages.size());
        assertContainsAll(list, messages);

        job.cancel();
    }

    @Test
    public void sourceTopic() {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.jmsTopic(() -> broker.createConnectionFactory(), destinationName))
                .map(TEXT_MESSAGE_FN)
                .drainTo(Sinks.list(listName));

        Job job = instance.newJob(pipeline, jobConfig());
        waitForJobRunning(job);

        List<String> messages = sendMessages(false);

        IListJet<String> list = instance.getList(listName);
        assertEqualsEventually(list::size, MESSAGE_COUNT);
        assertContainsAll(list, messages);

        job.cancel();
    }

    @Test
    public void sinkQueue() throws JMSException {
        populateList();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.list(listName))
                .drainTo(Sinks.jmsQueue(() -> broker.createConnectionFactory(), destinationName));

        List<String> messages = consumeMessages(true);
        Job job = instance.newJob(pipeline, jobConfig());

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(instance.getList(listName), messages);

        job.cancel();
    }

    @Test
    public void sinkTopic() throws JMSException {
        populateList();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.list(listName))
                .drainTo(Sinks.jmsTopic(() -> broker.createConnectionFactory(), destinationName));

        List<String> messages = consumeMessages(false);
        Job job = instance.newJob(pipeline, jobConfig());

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(instance.getList(listName), messages);

        job.cancel();
    }

    @Test
    public void sourceQueue_withBuilder() {
        StreamSource<String> source = JmsSourceBuilder.<String>builder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .projectionFn(TEXT_MESSAGE_FN)
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(source).drainTo(Sinks.list(listName));

        Job job = instance.newJob(pipeline, jobConfig());

        List<String> messages = sendMessages(true);

        IListJet<String> list = instance.getList(listName);
        assertEqualsEventually(list::size, messages.size());
        assertContainsAll(list, messages);

        job.cancel();
    }

    @Test
    public void sourceTopic_withBuilder() {
        StreamSource<String> source = JmsSourceBuilder.<String>builder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .topic()
                .projectionFn(TEXT_MESSAGE_FN)
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(source).drainTo(Sinks.list(listName));

        Job job = instance.newJob(pipeline, jobConfig());
        waitForJobRunning(job);

        List<String> messages = sendMessages(false);

        IListJet<String> list = instance.getList(listName);
        assertEqualsEventually(list::size, messages.size());
        assertContainsAll(list, messages);

        job.cancel();
    }

    @Test
    public void sinkQueue_withBuilder() throws JMSException {
        populateList();

        Sink<String> sink = JmsSinkBuilder.<String>builder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.<String>list(listName))
                .drainTo(sink);

        List<String> messages = consumeMessages(true);
        Job job = instance.newJob(pipeline, jobConfig());

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(instance.getList(listName), messages);

        job.cancel();
    }

    @Test
    public void sinkTopic_withBuilder() throws JMSException {
        populateList();

        Sink<String> sink = JmsSinkBuilder.<String>builder(() -> broker.createConnectionFactory())
                .destinationName(destinationName)
                .topic()
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.<String>list(listName))
                .drainTo(sink);

        List<String> messages = consumeMessages(false);
        Job job = instance.newJob(pipeline, jobConfig());

        assertEqualsEventually(messages::size, MESSAGE_COUNT);
        assertContainsAll(instance.getList(listName), messages);

        job.cancel();
    }

    private List<String> consumeMessages(boolean isQueue) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = broker.createConnectionFactory();
        Connection connection = connectionFactory.createConnection();
        connection.start();

        List<String> messages = synchronizedList(new ArrayList<>());
        spawn(() -> {
            try {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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


    private List<String> sendMessages(boolean isQueue) {
        return range(0, MESSAGE_COUNT)
                .mapToObj(i -> uncheckCall(() -> sendMessage(destinationName, isQueue)))
                .collect(toList());
    }

    private void populateList() {
        IListJet<String> list = instance.getList(listName);
        range(0, MESSAGE_COUNT).mapToObj(i -> randomString()).forEach(list::add);
    }

    private JobConfig jobConfig() {
        return new JobConfig().addClass(getClass());
    }

    private static void waitForJobRunning(Job job) {
        while (job.getStatus() != JobStatus.RUNNING) {
            sleepMillis(1);
        }
    }

    private String sendMessage(String destinationName, boolean isQueue) throws Exception {
        String message = randomString();

        ActiveMQConnectionFactory connectionFactory = broker.createConnectionFactory();
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        TextMessage textMessage = session.createTextMessage(message);
        producer.send(textMessage);
        session.close();
        connection.close();
        return message;
    }

}
