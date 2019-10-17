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

package com.hazelcast.jet.examples.jms;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnector;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.TextMessage;
import java.util.concurrent.CancellationException;

import static java.util.Collections.singleton;

/**
 * A pipeline which streams messages from a JMS queue, filters them according
 * to the priority and writes a new message with modified properties to another
 * JMS queue.
 */
public class JmsQueueSample {

    private static final String INPUT_QUEUE = "inputQueue";
    private static final String OUTPUT_QUEUE = "outputQueue";

    private EmbeddedActiveMQ embeddedActiveMQ;
    private JmsMessageProducer producer;
    private JetInstance jet;

    private Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jmsQueue(() -> new ActiveMQConnectionFactory("vm://0"), INPUT_QUEUE))
         .withoutTimestamps()
         .filter(message -> message.getJMSPriority() > 3)
         .map(message -> (TextMessage) message)
         // print the message text to the log
         .peek(TextMessage::getText)
         .writeTo(Sinks.<TextMessage>jmsQueueBuilder(() -> new ActiveMQConnectionFactory("vm://0"))
                 .destinationName(OUTPUT_QUEUE)
                 .messageFn((session, message) -> {
                         // create new text message with the same text and few additional properties
                         TextMessage textMessage = session.createTextMessage(message.getText());
                         textMessage.setBooleanProperty("isActive", true);
                         textMessage.setJMSPriority(8);
                         return textMessage;
                     }
                 )
                 .build());
        return p;
    }

    public static void main(String[] args) throws Exception {
        new JmsQueueSample().go();
    }

    private void go() throws Exception {
        try {
            setup();
            Job job = jet.newJob(buildPipeline());
            Thread.sleep(10000);
            job.cancel();
            try {
                job.join();
            } catch (CancellationException ignored) {
            }
        } finally {
            cleanup();
        }
    }

    private void setup() throws Exception {
        embeddedActiveMQ = new EmbeddedActiveMQ();
        embeddedActiveMQ.setConfiguration(new ConfigurationImpl()
                .setAcceptorConfigurations(singleton(new TransportConfiguration(InVMAcceptorFactory.class.getName())))
                .setPersistenceEnabled(false)
                .setSecurityEnabled(false));
        embeddedActiveMQ.start();

        producer = new JmsMessageProducer(INPUT_QUEUE, JmsMessageProducer.DestinationType.QUEUE);
        producer.start();

        jet = Jet.newJetInstance();
    }

    private void cleanup() throws Exception {
        producer.stop();
        Jet.shutdownAll();
        embeddedActiveMQ.stop();
        InVMConnector.resetThreadPool(); // without this the VM doesn't terminate
    }
}
