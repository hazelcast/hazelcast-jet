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

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.Collection;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.processor.SinkProcessors.writeBufferedP;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.stream.Collectors.toList;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;

/**
 * Private API. Access via {@link SinkProcessors#writeJmsQueueP} or {@link
 * SinkProcessors#writeJmsTopicP}.
 */
public final class WriteJmsP {

    private static final int PREFERRED_LOCAL_PARALLELISM = 4;

    private WriteJmsP() {
    }

    /**
     * Private API. Use {@link SinkProcessors#writeJmsQueueP} or {@link
     * SinkProcessors#writeJmsTopicP} instead
     */
    public static <T> ProcessorMetaSupplier supplier(
            SupplierEx<? extends Connection> newConnectionFn,
            BiFunctionEx<? super Session, T, ? extends Message> messageFn,
            String name,
            boolean isTopic
    ) {
        checkSerializable(newConnectionFn, "newConnectionFn");
        checkSerializable(messageFn, "messageFn");

        return ProcessorMetaSupplier.of(PREFERRED_LOCAL_PARALLELISM,
                new Supplier<>(newConnectionFn, messageFn, name, isTopic));
    }

    private static final class Supplier<T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final SupplierEx<? extends Connection> newConnectionFn;
        private final String name;
        private final boolean isTopic;
        private final BiFunctionEx<? super Session, ? super T, ? extends Message> messageFn;

        private transient Connection connection;

        private Supplier(SupplierEx<? extends Connection> newConnectionFn,
                         BiFunctionEx<? super Session, ? super T, ? extends Message> messageFn,
                         String name,
                         boolean isTopic
        ) {
            this.newConnectionFn = newConnectionFn;
            this.messageFn = messageFn;
            this.name = name;
            this.isTopic = isTopic;
        }

        @Override
        public void init(@Nonnull Context ignored) throws Exception {
            connection = newConnectionFn.get();
            connection.start();
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            FunctionEx<Processor.Context, JmsContext> createFn = jet -> {
                Session session = connection.createSession(false, AUTO_ACKNOWLEDGE);
                Destination destination = isTopic ? session.createTopic(name) : session.createQueue(name);
                MessageProducer producer = session.createProducer(destination);
                return new JmsContext(session, producer);
            };
            BiConsumerEx<JmsContext, T> onReceiveFn = (jmsContext, item) -> {
                Message message = messageFn.apply(jmsContext.session, item);
                // TODO we use synchronous send here
                jmsContext.producer.send(message);
            };
            ConsumerEx<JmsContext> destroyFn = jmsContext -> {
                jmsContext.producer.close();
                jmsContext.session.close();
            };
            SupplierEx<Processor> supplier = writeBufferedP(createFn, onReceiveFn, ConsumerEx.noop(), destroyFn);

            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public void close(Throwable error) throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        static class JmsContext {
            private final Session session;
            private final MessageProducer producer;

            JmsContext(Session session, MessageProducer producer) {
                this.session = session;
                this.producer = producer;
            }
        }
    }
}
