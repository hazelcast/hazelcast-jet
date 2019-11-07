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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.XAConnectionFactory;
import java.util.function.Function;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamJmsQueueP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamJmsTopicP;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * See {@link Sources#jmsQueueBuilder} or {@link Sources#jmsTopicBuilder}.
 *
 * @since 3.0
 */
public final class JmsSourceBuilder {

    private final SupplierEx<? extends ConnectionFactory> factorySupplier;
    private final boolean isTopic;

    private FunctionEx<? super ConnectionFactory, ? extends Connection> connectionFn;
    private FunctionEx<? super Session, ? extends MessageConsumer> consumerFn;

    private String username;
    private String password;
    private String destinationName;
    private ProcessingGuarantee maxGuarantee = ProcessingGuarantee.EXACTLY_ONCE;
    private boolean isSharedConsumer;

    /**
     * Use {@link Sources#jmsQueueBuilder} of {@link Sources#jmsTopicBuilder}.
     */
    JmsSourceBuilder(SupplierEx<? extends ConnectionFactory> factorySupplier, boolean isTopic) {
        checkSerializable(factorySupplier, "factorySupplier");
        this.factorySupplier = factorySupplier;
        this.isTopic = isTopic;
    }

    /**
     * Sets the connection parameters. If {@link #connectionFn(FunctionEx)} is
     * set, these parameters are ignored.
     */
    public JmsSourceBuilder connectionParams(String username, String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    /**
     * Sets the function which creates the connection using the connection
     * factory.
     * <p>
     * If not provided, this function is used:
     * <pre>
     *     connectionFn = factory -> factory instanceof XAConnectionFactory
     *            ? ((XAConnectionFactory) factory).createXAConnection(username, password)
     *            : factory.createConnection(username, password);
     * </pre>
     * That means it creates an XA connection if the factory is an XA factory.
     * The user name and password set with {@link #connectionParams} are used.
     */
    public JmsSourceBuilder connectionFn(
            @Nonnull FunctionEx<? super ConnectionFactory, ? extends Connection> connectionFn
    ) {
        checkSerializable(connectionFn, "connectionFn");
        this.connectionFn = connectionFn;
        return this;
    }

    /**
     * Sets the name of the destination (name of the topic or queue). If {@code
     * consumerFn} is provided, this parameter is ignored.
     */
    public JmsSourceBuilder destinationName(String destinationName) {
        this.destinationName = destinationName;
        return this;
    }

    /**
     * Sets the function which creates the message consumer from session.
     * <p>
     * If not provided, {@code Session#createConsumer(destinationName)} is used
     * to create the consumer. See {@link #destinationName(String)}.
     */
    public JmsSourceBuilder consumerFn(
            @Nonnull FunctionEx<? super Session, ? extends MessageConsumer> consumerFn
    ) {
        checkSerializable(consumerFn, "consumerFn");
        this.consumerFn = consumerFn;
        return this;
    }

    /**
     * Sets the maximum processing guarantee for the source. You can use it to
     * decrease the guarantee of this source compared to the job's guarantee.
     * If you configure stronger guarantee than the job has, the job's
     * guarantee will be used. Use it if you want to avoid the overhead of
     * acknowledging the messages in transactions if you don't need it.
     * <p>
     * XA transactions are required for exactly-once mode. If your job uses
     * exactly-once guarantee and your JMS client doesn't support XA
     * transactions, use {@code AT_LEAST_ONCE} guarantee to use normal
     * transactions. In this mode the transaction will be committed in the 2nd
     * phase of the snapshot so you're guaranteed that each message will be
     * processed at least once if the job is forced to restart.
     * <p>
     * If you use {@link ProcessingGuarantee#NONE}, messages will be consumed
     * in auto-acknowledge mode. In this mode some messages can be processed
     * more than once and some not at all if the job is forced to restart.
     * <p>
     * The default is {@link ProcessingGuarantee#EXACTLY_ONCE}, which means
     * that source's guarantee will match job's guarantee.
     *
     * @return this instance for fluent API
     */
    public JmsSourceBuilder maxGuarantee(ProcessingGuarantee guarantee) {
        maxGuarantee = guarantee;
        return this;
    }

    /**
     * Specifies whether the MessageConsumer of the JMS topic is shared, that
     * is when {@code createSharedConsumer()} or {@code
     * createSharedDurableConsumer()} was used to create it in the {@link
     * #consumerFn(FunctionEx)}.
     * <p>
     * If the consumer is not shared, only single processor on single member
     * will connect to the broker to receive the messages. If you set this
     * parameter to {@code true} for a non-shared consumer, each message will
     * be emitted duplicately on each member.
     * <p>
     * The consumer for a queue is always assumed to be shared, regardless of
     * this setting.
     * <p>
     * The default value is {@code false}.
     *
     * @return this instance for fluent API
     */
    public JmsSourceBuilder sharedConsumer(boolean isSharedConsumer) {
        this.isSharedConsumer = isSharedConsumer;
        return this;
    }

    /**
     * Creates and returns the JMS {@link StreamSource} with the supplied
     * components and the projection function {@code projectionFn}.
     *
     * @param projectionFn the function which creates output object from each
     *                    message
     * @param <T> the type of the items the source emits
     */
    public <T> StreamSource<T> build(@Nonnull FunctionEx<? super Message, ? extends T> projectionFn) {
        String usernameLocal = username;
        String passwordLocal = password;
        String destinationLocal = destinationName;
        @SuppressWarnings("UnnecessaryLocalVariable")
        boolean isTopicLocal = isTopic;

        if (connectionFn == null) {
            connectionFn = factory -> factory instanceof XAConnectionFactory
                    ? ((XAConnectionFactory) factory).createXAConnection(usernameLocal, passwordLocal)
                    : factory.createConnection(usernameLocal, passwordLocal);
        }
        if (consumerFn == null) {
            checkNotNull(destinationLocal, "neither consumerFn nor destinationName set");
            consumerFn = session -> session.createConsumer(isTopicLocal
                    ? session.createTopic(destinationLocal)
                    : session.createQueue(destinationLocal));
        }

        FunctionEx<? super ConnectionFactory, ? extends Connection> connectionFnLocal = connectionFn;
        @SuppressWarnings("UnnecessaryLocalVariable")
        SupplierEx<? extends ConnectionFactory> factorySupplierLocal = factorySupplier;
        SupplierEx<? extends Connection> newConnectionFn =
                () -> connectionFnLocal.apply(factorySupplierLocal.get());

        Function<EventTimePolicy<? super T>, ProcessorMetaSupplier> metaSupplierFactory =
                policy -> isTopic
                       ? streamJmsTopicP(newConnectionFn, consumerFn, isSharedConsumer, projectionFn, policy, maxGuarantee)
                       : streamJmsQueueP(newConnectionFn, consumerFn, projectionFn, policy, maxGuarantee);
        return Sources.streamFromProcessorWithWatermarks(sourceName(), true, metaSupplierFactory);
    }

    /**
     * Convenience for {@link JmsSourceBuilder#build(FunctionEx)}.
     */
    public StreamSource<Message> build() {
        return build(message -> message);
    }

    private String sourceName() {
        return (isTopic ? "jmsTopicSource(" : "jmsQueueSource(")
                + (destinationName == null ? "?" : destinationName) + ')';
    }
}
