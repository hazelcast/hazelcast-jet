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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.stream.IntStream.range;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;

/**
 * Private API. Access via {@link SourceProcessors#streamJmsQueueP} or {@link
 * SourceProcessors#streamJmsTopicP}
 * <p>
 * Since we use a non-blocking version of JMS consumer API, the processor is
 * marked as cooperative.
 */
public class StreamJmsP<T> extends AbstractProcessor {

    public static final int PREFERRED_LOCAL_PARALLELISM = 4;

    private final Connection connection;
    private final FunctionEx<? super Session, ? extends MessageConsumer> consumerFn;
    private final FunctionEx<? super Message, ? extends T> projectionFn;
    private final EventTimeMapper<? super T> eventTimeMapper;

    private Session session;
    private MessageConsumer consumer;
    private Traverser<Object> traverser;

    StreamJmsP(
            Connection connection,
            FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
            FunctionEx<? super Message, ? extends T> projectionFn,
            EventTimePolicy<? super T> eventTimePolicy
    ) {
        this.connection = connection;
        this.consumerFn = consumerFn;
        this.projectionFn = projectionFn;

        eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(1);
    }

    /**
     * Private API. Use {@link SourceProcessors#streamJmsQueueP} or {@link
     * SourceProcessors#streamJmsTopicP} instead.
     */
    @Nonnull
    public static <T> ProcessorSupplier supplier(
            @Nonnull SupplierEx<? extends Connection> newConnectionFn,
            @Nonnull FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
            @Nonnull FunctionEx<? super Message, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        checkSerializable(newConnectionFn, "newConnectionFn");
        checkSerializable(consumerFn, "consumerFn");
        checkSerializable(projectionFn, "projectionFn");

        return new Supplier<>(newConnectionFn, consumerFn, projectionFn, eventTimePolicy);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        session = connection.createSession(false, AUTO_ACKNOWLEDGE);
        consumer = consumerFn.apply(session);
        traverser = ((Traverser<Message>) () -> uncheckCall(() -> consumer.receiveNoWait()))
                .flatMap(t -> eventTimeMapper.flatMapEvent(projectionFn.apply(t), 0, handleJmsTimestamp(t)));
    }

    private static long handleJmsTimestamp(Message msg) {
        try {
            // as per `getJMSTimestamp` javadoc, it can return 0 if the timestamp was optimized away
            return msg.getJMSTimestamp() == 0 ? EventTimeMapper.NO_NATIVE_TIME : msg.getJMSTimestamp();
        } catch (JMSException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public boolean complete() {
        emitFromTraverser(traverser);
        return false;
    }

    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
        if (session != null) {
            session.close();
        }
    }

    private static final class Supplier<T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final SupplierEx<? extends Connection> newConnectionFn;
        private final FunctionEx<? super Session, ? extends MessageConsumer> consumerFn;
        private final FunctionEx<? super Message, ? extends T> projectionFn;
        private final EventTimePolicy<? super T> eventTimePolicy;

        private transient Connection connection;

        private Supplier(SupplierEx<? extends Connection> newConnectionFn,
                         FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
                         FunctionEx<? super Message, ? extends T> projectionFn,
                         EventTimePolicy<? super T> eventTimePolicy
        ) {
            this.newConnectionFn = newConnectionFn;
            this.consumerFn = consumerFn;
            this.projectionFn = projectionFn;
            this.eventTimePolicy = eventTimePolicy;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            connection = newConnectionFn.get();
            connection.start();
        }

        @Override
        public void close(@Nullable Throwable error) throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return range(0, count)
                    .mapToObj(i ->
                            new StreamJmsP<>(connection, consumerFn, projectionFn, eventTimePolicy))
                    .collect(Collectors.toList());
        }
    }
}
