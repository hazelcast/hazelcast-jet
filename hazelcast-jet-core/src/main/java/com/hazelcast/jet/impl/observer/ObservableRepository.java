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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetProperties;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.topic.ITopic;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static java.lang.Math.min;

public class ObservableRepository {

    private static final String COMPLETED_OBSERVABLES_LIST_NAME = INTERNAL_JET_OBJECTS_PREFIX + "completedObservables";
    private static final int MAX_CLEANUP_ATTEMPTS_AT_ONCE = 10;

    private final HazelcastInstance instance;
    private final IList<Tuple2<String, Long>> completedObservables;
    private final long expirationTime;

    public ObservableRepository(JetInstance jetInstance, JetConfig jetConfig) {
        this.instance = jetInstance.getHazelcastInstance();
        this.completedObservables = instance.getList(COMPLETED_OBSERVABLES_LIST_NAME);
        this.expirationTime = getExpirationTime(jetConfig);
    }

    public void completeObservables(Set<String> observables, Throwable error) {
        ObservableBatch notification = error == null ? ObservableBatch.endOfData() : ObservableBatch.error(error);
        for (String observable : observables) {
            completedObservables.add(Tuple2.tuple2(observable, System.currentTimeMillis()));

            ITopic<ObservableBatch> topic = instance.getTopic(observable);
            topic.publish(notification);
        }
    }

    public void cleanup() {
        //attempt clean-up just on the last few items of the completion list
        //we don't want to spend a lot of time on this in one go
        int total = completedObservables.size();
        int toClean = min(MAX_CLEANUP_ATTEMPTS_AT_ONCE, total);
        List<Tuple2<String, Long>> cleanList = completedObservables.subList(total - toClean, total);

        long currentTime = System.currentTimeMillis();
        Iterator<Tuple2<String, Long>> tupleIterator = cleanList.iterator();
        while (tupleIterator.hasNext()) {
            Tuple2<String, Long> tuple2 = tupleIterator.next();
            long completionTime = tuple2.getValue();
            boolean expired = currentTime - completionTime > expirationTime;
            if (expired) {
                instance.getTopic(tuple2.getKey()).destroy();
                tupleIterator.remove();
            }
        }
    }

    private static long getExpirationTime(JetConfig jetConfig) {
        //we will keep observables for the same amount of time as job results
        HazelcastProperties hazelcastProperties = new HazelcastProperties(jetConfig.getProperties());
        return hazelcastProperties.getMillis(JetProperties.JOB_RESULTS_TTL_SECONDS);
    }

}
