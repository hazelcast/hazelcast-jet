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
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.execution.DoneItem;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.function.LongSupplier;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.query.Predicates.lessEqual;

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
    public static final String JET_OBSERVABLE_NAME_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "observables.";

    /**
     * Map holding name-completion time pairs for completed observable
     * until they expire and are cleaned up.
     */
    static final String COMPLETED_OBSERVABLES_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "completedObservables";

    private final HazelcastInstance instance;
    private final IMap<String, Long> completedObservables;

    // expiry TTL in millis
    private final long ttlMillis;

    private final LongSupplier clock;
    private final ILogger logger;

    public ObservableRepository(HazelcastInstance instance, long ttlMillis) {
        this(instance, ttlMillis, System::currentTimeMillis);
    }

    ObservableRepository(HazelcastInstance instance, long ttlMillis, LongSupplier clock) {
        this.completedObservables = instance.getMap(COMPLETED_OBSERVABLES_MAP_NAME);
        this.instance = instance;
        this.ttlMillis = ttlMillis;
        this.logger = instance.getLoggingService().getLogger(ObservableRepository.class);
        this.clock = clock;
    }

    @Nonnull
    public static String ringbufferName(String observableName) {
        return JET_OBSERVABLE_NAME_PREFIX + observableName;
    }

    public void init(Collection<String> observables) {
        for (String observable : observables) {
            completedObservables.remove(observable);
        }
    }

    public void complete(Collection<String> observables, Throwable error) {
        for (String observable : observables) {
            String ringbufferName = ringbufferName(observable);
            Ringbuffer<Object> ringbuffer = instance.getRingbuffer(ringbufferName);
            Object completion = error == null ? DoneItem.DONE_ITEM : WrappedThrowable.of(error);
            ringbuffer.addAsync(completion, OverflowPolicy.OVERWRITE);
            completedObservables.put(observable, clock.getAsLong());
        }
    }

    public static void destroy(HazelcastInstance instance, String observable) {
        instance.getRingbuffer(ringbufferName(observable)).destroy();
    }

    public void cleanup() {
        long limit = clock.getAsLong() - this.ttlMillis;
        try {
            completedObservables.executeOnEntries(
                    ObservableRepository::removeKey, lessEqual("this", limit)
            ).forEach((o, x) -> destroy(instance, o));
        } catch (Exception e) {
            logger.warning("Failed cleaning-up expired observables", e);
        }
    }

    private static Object removeKey(Entry<String, Long> e) {
        e.setValue(null);
        return null;
    }
}
