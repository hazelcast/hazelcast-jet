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

import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetProperties;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.execution.DoneItem;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;

public class ObservableRepository {

    //TODO (PR-1729): update javadoc for auto-cleanup functionality

    /**
     * Constant ID to be used as a {@link ProcessorMetaSupplier#getTags()
     * PMS tag key} for specifying when a PMS owns an {@link Observable} (ie.
     * is the entity populating the {@link Observable} with data).
     */
    public static final String OWNED_OBSERVABLE = "owned_observable";

    /**
     * Map holding name-completion time pairs for completed observable
     * until they expire and are cleaned up.
     */
    protected static final String COMPLETED_OBSERVABLES_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "completedObservables";

    /**
     * Prefix of all topic names used to back {@link Observable} implementations,
     * necessary so that such topics can't clash with regular topics used
     * for other purposes.
     */
    private static final String JET_OBSERVABLE_NAME_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "observables.";

    /**
     * Name of distributed executor service that will handle cleaning up
     * expired ringbuffers.
     */
    private static final String CLEANUP_SERVICE_NAME = INTERNAL_JET_OBJECTS_PREFIX + "observables_cleanup";

    private static final MemberSelector ALL_MEMBER_SELECTOR = member -> true;

    private final HazelcastInstance hzInstance;
    private final IExecutorService cleanupService;
    private final IMap<String, Long> completedObservables;
    private final long expirationInterval;
    private final LongSupplier timeSource;
    private final CleanupTask cleanupTask = new CleanupTask();
    private final MemberSelector memberSelector;

    public ObservableRepository(JetInstance jet) {
        this(jet, System::currentTimeMillis, ALL_MEMBER_SELECTOR);
    }

    ObservableRepository(JetInstance jet, LongSupplier timeSource, MemberSelector memberSelector) {
        this.hzInstance = jet.getHazelcastInstance();
        this.cleanupService = hzInstance.getExecutorService(CLEANUP_SERVICE_NAME);
        this.completedObservables = hzInstance.getMap(COMPLETED_OBSERVABLES_MAP_NAME);
        this.expirationInterval = getExpirationInterval(jet.getConfig());
        this.timeSource = timeSource;
        this.memberSelector = memberSelector;
    }

    private static long getExpirationInterval(JetConfig jetConfig) {
        //we will keep observables for the same amount of time as job results
        HazelcastProperties hazelcastProperties = new HazelcastProperties(jetConfig.getProperties());
        return hazelcastProperties.getMillis(JetProperties.JOB_RESULTS_TTL_SECONDS);
    }

    @Nonnull
    public static String getRingbufferName(String observableName) {
        return JET_OBSERVABLE_NAME_PREFIX + observableName;
    }

    public static void destroyRingbuffer(String observable, HazelcastInstance hzInstance) {
        String ringbufferName = getRingbufferName(observable);
        hzInstance.getRingbuffer(ringbufferName).destroy();
    }

    public void initObservables(Collection<String> observables) {
        for (String observable : observables) {
            completedObservables.remove(observable);
        }
    }

    public void completeObservables(Collection<String> observables, Throwable error) {
        for (String observable : observables) {
            String ringbufferName = getRingbufferName(observable);
            Ringbuffer<Object> ringbuffer = hzInstance.getRingbuffer(ringbufferName);
            Object completion = error == null ? DoneItem.DONE_ITEM : WrappedThrowable.of(error);
            ringbuffer.addAsync(completion, OverflowPolicy.OVERWRITE);

            completedObservables.put(observable, timeSource.getAsLong());
        }
    }

    public void cleanup() {
        cleanupTask.setExpirationLimit(timeSource.getAsLong() - expirationInterval);

        //trigger local cleanup on a single random member (all will get their turn eventually)
        cleanupService.execute(cleanupTask, memberSelector);
    }

    public static class CleanupTask implements Runnable, Serializable, HazelcastInstanceAware {

        private static final long serialVersionUID = 0L;

        private long expirationLimit = -1;

        private transient HazelcastInstance hzInstance;

        public void setExpirationLimit(long expirationLimit) {
            this.expirationLimit = expirationLimit;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hzInstance = hazelcastInstance;
        }

        @Override
        public void run() {
            try {
                IMap<String, Long> completedObservables = hzInstance.getMap(COMPLETED_OBSERVABLES_MAP_NAME);
                Set<Map.Entry<String, Long>> localEntries = getLocalEntries(completedObservables);
                for (Map.Entry<String, Long> entry : localEntries) {
                    Long completionTime = entry.getValue();
                    if (completionTime <= expirationLimit) {
                        String observable = entry.getKey();
                        completedObservables.remove(observable);
                        hzInstance.getRingbuffer(getRingbufferName(observable)).destroy();
                    }
                }
            } catch (Exception e) {
                ILogger logger = hzInstance.getLoggingService().getLogger(CleanupTask.class);
                logger.warning("Failed cleaning-up expired observables, reason: " + e.getMessage(), e);
            }
        }

        @Nonnull
        private static Set<Map.Entry<String, Long>> getLocalEntries(IMap<String, Long> map) {
            return map.getAll(map.localKeySet()).entrySet();
        }
    }

}
