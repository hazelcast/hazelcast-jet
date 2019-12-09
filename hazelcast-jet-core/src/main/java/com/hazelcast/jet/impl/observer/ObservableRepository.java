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

package com.hazelcast.jet.impl.observer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.ReliableMessageListener;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;

public final class ObservableRepository {

    /**
     * Constant ID to be used as a {@link ProcessorMetaSupplier#getTags()
     * PMS tag key} for specifying when a PMS owns an {@link Observable} (ie.
     * is the entity populating the {@link Observable} with data).
     */
    public static final String OWNED_OBSERVABLE = "owned_observable";

    /**
     * Prefix of all topic names used to back {@link Observable} implementations,
     * necessary so that such topics can't clash with regular topics used
     * for other purposes.
     */
    private static final String JET_OBSERVABLE_NAME_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "observable.";

    private ObservableRepository() {
    }

    public static void initObservable(
            String observable,
            Consumer<ObservableBatch> onNewMessage,
            Consumer<Long> onSequenceNo,
            JetInstance jet
    ) {
        ITopic<ObservableBatch> topic = getTopic(jet.getHazelcastInstance(), observable);
        topic.addMessageListener(new TopicListener(onNewMessage, onSequenceNo));
    }

    public static void destroyObservable(String observable, HazelcastInstance hzInstance) {
        ITopic<ObservableBatch> topic = getTopic(hzInstance, observable);
        topic.destroy();
    }

    public static void completeObservables(Collection<String> observables, Throwable error, HazelcastInstance hzInstance) {
        for (String observable : observables) {
            ITopic<ObservableBatch> topic = getTopic(hzInstance, observable);
            topic.publish(error == null ? ObservableBatch.endOfData() : ObservableBatch.error(error));
        }
    }

    public static FunctionEx<HazelcastInstance, ConsumerEx<ArrayList<Object>>> getPublishFn(String name) {
        return instance -> {
            ITopic<ObservableBatch> topic = getTopic(instance, name);
            return buffer -> {
                topic.publish(ObservableBatch.items(buffer));
                buffer.clear();
            };
        };
    }

    @Nonnull
    private static ITopic<ObservableBatch> getTopic(HazelcastInstance intance, String observableName) {
        String topicName = JET_OBSERVABLE_NAME_PREFIX + observableName;
        return intance.getReliableTopic(topicName);
    }

    private static final class TopicListener implements ReliableMessageListener<ObservableBatch> {

        private final Consumer<ObservableBatch> msgConsumer;
        private final Consumer<Long> seqNoConsumer;

        TopicListener(Consumer<ObservableBatch> msgConsumer, Consumer<Long> seqNoConsumer) {
            this.msgConsumer = msgConsumer;
            this.seqNoConsumer = seqNoConsumer;
        }

        @Override
        public long retrieveInitialSequence() {
            //We want to start with the next published message.
            return -1;
        }

        @Override
        public void storeSequence(long sequence) {
            seqNoConsumer.accept(sequence);
        }

        @Override
        public boolean isLossTolerant() {
            //We don't want to stop receiving events if there is loss.
            return true;
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            //Listening to the topic should be terminated if there is an exception thrown while processing a message.
            return true;
        }

        @Override
        public void onMessage(Message<ObservableBatch> message) {
            msgConsumer.accept(message.getMessageObject());
        }
    }

}
