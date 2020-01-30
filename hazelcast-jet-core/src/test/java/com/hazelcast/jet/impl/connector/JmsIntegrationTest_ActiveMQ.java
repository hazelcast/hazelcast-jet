/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.SupplierEx;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.ClassRule;

import javax.jms.ConnectionFactory;

public class JmsIntegrationTest_ActiveMQ extends JmsIntegrationTestBase {

    @ClassRule
    public static EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    private static final SupplierEx<ConnectionFactory> FACTORY_SUPPLIER =
            () -> new ActiveMQConnectionFactory(broker.getVmURL());

    @Override
    protected SupplierEx<ConnectionFactory> getConnectionFactory() {
        return FACTORY_SUPPLIER;
    }
}
