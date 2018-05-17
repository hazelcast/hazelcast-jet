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

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.function.DistributedFunction;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.net.URI;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;

/**
 * todo add proper javadoc
 */
public class JmsTestSupport extends JetTestSupport {

    static final String BROKER_URL = "tcp://localhost:61616";
    static final File BROKER_DIR = new File("activemq-data");
    static final DistributedFunction<Connection, Session> SESSION_F =
            c -> uncheckCall(() -> c.createSession(false, Session.AUTO_ACKNOWLEDGE));
    static final DistributedFunction<Message, String> TEXT_MESSAGE_F =
            m -> uncheckCall(((TextMessage) m)::getText);


    static BrokerService broker;

    static Connection connection;

    @BeforeClass
    public static void setup() throws Exception {
        cleanDirectory(BROKER_DIR);

        broker = BrokerFactory.createBroker(new URI("broker:(" + BROKER_URL + ")"));
        broker.start();

        connection = new ActiveMQConnectionFactory(BROKER_URL).createConnection();
        connection.start();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        connection.close();
        broker.stop();
        broker.waitUntilStopped();
        cleanDirectory(BROKER_DIR);
    }

    static String sendMessage(String destinationName, boolean isQueue) throws Exception {
        String message = randomString();

        Session session = SESSION_F.apply(connection);
        Destination destination = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        TextMessage textMessage = session.createTextMessage(message);
        producer.send(textMessage);
        session.close();
        return message;
    }

    private static void cleanDirectory(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    cleanDirectory(f);
                }
            }
        }
        assert file.delete();
    }
}
