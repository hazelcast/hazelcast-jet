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

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetProperties;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.ReliableMessageListener;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static java.lang.Math.min;

public class ObservableRepository {

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

    private static final String COMPLETED_OBSERVABLES_LIST_NAME = INTERNAL_JET_OBJECTS_PREFIX + "completedObservables";
    private static final int MAX_CLEANUP_ATTEMPTS_AT_ONCE = 10;

    private final JetInstance jet;
    private final IList<Tuple2<String, Long>> completedObservables;
    private final long expirationTime;
    private final int maxSize;
    private final LongSupplier timeSource;

    public ObservableRepository(JetInstance jet, JetConfig config) {
        this(jet, config, System::currentTimeMillis);
    }

    ObservableRepository(JetInstance jet, JetConfig config, LongSupplier timeSource) {
        this.jet = jet;
        this.completedObservables = jet.getList(COMPLETED_OBSERVABLES_LIST_NAME);
        this.expirationTime = getExpirationTime(config);
        this.maxSize = getMaxSize(config);
        this.timeSource = timeSource;
    }

    public static UUID initObservable(
            String observable,
            Consumer<ObservableBatch> onNewMessage,
            Consumer<Long> onSequenceNo,
            JetInstance jet
    ) {
        ITopic<ObservableBatch> topic = getTopic(jet.getHazelcastInstance(), observable);
        return topic.addMessageListener(new TopicListener(onNewMessage, onSequenceNo));
    }

    public static void destroyObservable(
            String observable,
            UUID registrationId,
            JetInstance jet
    ) {
        ITopic<ObservableBatch> topic = getTopic(jet.getHazelcastInstance(), observable);
        topic.removeMessageListener(registrationId);
        topic.destroy();
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

    static void completeObservable(String observable, Throwable error, JetInstance jet, LongSupplier timeSource) {
        ITopic<ObservableBatch> topic = getTopic(jet.getHazelcastInstance(), observable);
        topic.publish(error == null ? ObservableBatch.endOfData() : ObservableBatch.error(error));

        IList<Tuple2<String, Long>> completedObservables =
                jet.getList(ObservableRepository.COMPLETED_OBSERVABLES_LIST_NAME);
        completedObservables.add(Tuple2.tuple2(observable, timeSource.getAsLong()));
    }

    private static long getExpirationTime(JetConfig jetConfig) {
        //we will keep observables for the same amount of time as job results
        HazelcastProperties hazelcastProperties = new HazelcastProperties(jetConfig.getProperties());
        return hazelcastProperties.getMillis(JetProperties.JOB_RESULTS_TTL_SECONDS);
    }

    private static int getMaxSize(JetConfig jetConfig) {
        //we will keep only as many observables as we do job results
        HazelcastProperties hazelcastProperties = new HazelcastProperties(jetConfig.getProperties());
        return hazelcastProperties.getInteger(JetProperties.JOB_RESULTS_MAX_SIZE);
    }

    @Nonnull
    private static ITopic<ObservableBatch> getTopic(HazelcastInstance intance, String observableName) {
        String topicName = JET_OBSERVABLE_NAME_PREFIX + observableName;
        return intance.getReliableTopic(topicName);
    }

    public void completeObservables(Collection<String> observables, Throwable error) {
        for (String observable : observables) {
            completeObservable(observable, error, jet, System::currentTimeMillis);
        }
    }

    public void cleanup() {
        int cleaned = cleanSomeExpired();
        if (cleaned < MAX_CLEANUP_ATTEMPTS_AT_ONCE && completedObservables.size() > maxSize) { //TODO (PR-1729): unit test
            int cleaningSlotsRemaining = MAX_CLEANUP_ATTEMPTS_AT_ONCE - cleaned;
            int excessSize = completedObservables.size() - maxSize;
            cleanRegardless(min(cleaningSlotsRemaining, excessSize));
        }
    }

    private int cleanSomeExpired() {
        long currentTime = timeSource.getAsLong();
        int cleaned = 0;
        Iterator<Tuple2<String, Long>> iterator = completedObservables.iterator();
        while (iterator.hasNext() && cleaned < MAX_CLEANUP_ATTEMPTS_AT_ONCE) {
            Tuple2<String, Long> tuple2 = iterator.next();
            long completionTime = tuple2.getValue();
            boolean expired = currentTime - completionTime >= expirationTime;
            if (expired) {
                destroyTopic(tuple2);
                iterator.remove();
                cleaned++;
            } else {
                return cleaned;
            }
        }
        return cleaned;
    }

    private void cleanRegardless(int count) {
        Iterator<Tuple2<String, Long>> iterator = completedObservables.iterator();
        for (int i = 0; i < count; i++) {
            destroyTopic(iterator.next());
            iterator.remove();
        }
    }

    private void destroyTopic(Tuple2<String, Long> tuple2) {
        String observableName = tuple2.getKey();
        ITopic<ObservableBatch> topic = getTopic(jet.getHazelcastInstance(), observableName);
        topic.destroy();
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
