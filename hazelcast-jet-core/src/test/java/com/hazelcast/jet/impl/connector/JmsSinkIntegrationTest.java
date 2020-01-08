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
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
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

import static javax.jms.Session.DUPS_OK_ACKNOWLEDGE;

public class JmsSinkIntegrationTest extends SimpleTestInClusterSupport {
    @ClassRule
    public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    // TODO [viliam] more tests

    @Test
    public void test_transactional_withRestarts_graceful() throws Exception {
        test_transactional_withRestarts(true);
    }

    @Test
    public void test_transactional_withRestarts_forceful() throws Exception {
        test_transactional_withRestarts(false);
    }

    private void test_transactional_withRestarts(boolean graceful) throws Exception {
        String destinationName = randomString();
        Sink<Integer> sink = Sinks.jmsQueue(destinationName, () -> new ActiveMQXAConnectionFactory(broker.getVmURL()));

        try (
                Connection connection = broker.createConnectionFactory().createConnection();
                Session session = connection.createSession(false, DUPS_OK_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(session.createQueue(destinationName))
        ) {
            connection.start();
            Set<Integer> actualSinkContents = new HashSet<>();
            ExactlyOnceSinkTestUtil.test_transactional_withRestarts(instance(), logger, sink, graceful, () -> {
                for (Message msg; (msg = consumer.receiveNoWait()) != null; ) {
                    actualSinkContents.add(Integer.valueOf(((TextMessage) msg).getText()));
                }
                return actualSinkContents;
            });
        }
    }
}
