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

import com.hazelcast.jet.impl.util.ExceptionUtil;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import java.net.URI;

/**
 * Utility class to start/stop an ActiveMQ Broker instance
 */
public final class ActiveMQBroker {

    public static final String BROKER_URL = "tcp://localhost:61616";

    private BrokerService broker;

    ActiveMQBroker() throws Exception {
        this.broker = BrokerFactory.createBroker(new URI("broker:(" + BROKER_URL + ")"));
    }

    public void start() {
        try {
            broker.start();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void stop() {
        try {
            broker.stop();
            broker.waitUntilStopped();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
