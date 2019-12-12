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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.DAYS;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;

final class JmsTestUtil {
    private JmsTestUtil() { }

    static EmbeddedActiveMQResource createActiveMqResource() {
        ConfigurationImpl config = new ConfigurationImpl()
                .setName("embedded-server")
                .setPersistenceEnabled(false)
                .setSecurityEnabled(false)
                // use long timeout - if we don't correctly roll back, it should show
                .setTransactionTimeout(DAYS.toMillis(1))
                .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()))
                .addAddressesSetting("#", new AddressSettings()
                        .setDeadLetterAddress(SimpleString.toSimpleString("dla"))
                        .setMaxDeliveryAttempts(1000)
                        .setExpiryAddress(SimpleString.toSimpleString("expiry")));
        return new EmbeddedActiveMQResource(config);
    }

    static List<Object> sendMessages(ConnectionFactory cf, String destinationName, boolean isQueue, int count)
            throws JMSException {
        try (
                Connection conn = cf.createConnection();
                Session session = conn.createSession(false, AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(
                        isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName))
        ) {
            List<Object> res = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String message = "msg-" + i;
                producer.send(session.createTextMessage(message));
                res.add(message);
            }
            return res;
        }
    }
}
