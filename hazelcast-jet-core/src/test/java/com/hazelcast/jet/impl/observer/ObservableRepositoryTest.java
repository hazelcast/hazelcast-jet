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

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static com.hazelcast.jet.core.JetProperties.JOB_RESULTS_TTL_SECONDS;
import static com.hazelcast.jet.core.JetProperties.JOB_SCAN_PERIOD;

public class ObservableRepositoryTest extends SimpleTestInClusterSupport {

    private static final int MEMBER_COUNT = 2;
    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors();

    private ObservableRepository repository;
    private TestTimeSource timeSource;

    @BeforeClass
    public static void beforeClass() {
        JetConfig config = new JetConfig();
        Properties properties = config.getProperties();
        properties.setProperty(JOB_RESULTS_TTL_SECONDS.getName(), Long.toString(10));
        properties.setProperty(JOB_SCAN_PERIOD.getName(), Long.toString(Long.MAX_VALUE));

        initializeWithClient(MEMBER_COUNT, config, null);
    }

    @Before
    public void before() {
        TestProcessors.reset(MEMBER_COUNT * PARALLELISM);

        timeSource = new TestTimeSource();

        NodeEngineImpl nodeEngine = ((HazelcastInstanceImpl) instance().getHazelcastInstance()).node.getNodeEngine();
        MemberSelector memberSelector = new RoundRobinMemberSelector(nodeEngine);
        repository = new ObservableRepository(instance(), timeSource, memberSelector);
    }

    @Test
    public void cleanupAfterExpiration() {
        initObservables("o1", "o2", "o3", "o4", "o5");

        //when
        completeObservables("o1", "o2");
        timeSource.inc(1);                //time is: 1 sec
        cleanup();
        //then
        assertCompletedObservables("o1", "o2");                      //none have expired yet

        //when
        completeObservables("o3");
        timeSource.inc(1);                //time is: 2 sec
        cleanup();
        //then
        assertCompletedObservables("o1", "o2", "o3");                //none have expired yet

        //when
        completeObservables("o4", "o5");
        cleanup();
        //then
        assertCompletedObservables("o1", "o2", "o3", "o4", "o5");    //none have expired yet

        //when
        timeSource.inc(8);                //time is: 10 sec
        cleanup();
        //then
        assertCompletedObservables("o3", "o4", "o5");                //some have expired

        //when
        timeSource.inc(1);                //time is: 11 sec
        cleanup();
        //then
        assertCompletedObservables("o4", "o5");                      //more have expired

        //when
        timeSource.inc(1);                //time is: 12 sec
        cleanup();
        //then
        assertCompletedObservables();                                //all have expired
    }

    @Test
    public void completionsResetsExpiration() {
        initObservables("o1", "o2", "o3", "o4", "o5");

        //when
        completeObservables("o1", "o2", "o3", "o4");
        timeSource.inc(3);                //time is: 3 sec
        cleanup();
        //then
        assertCompletedObservables("o1", "o2", "o3", "o4");          //none have expired yet

        //when
        completeObservables("o2");
        timeSource.inc(1);                //time is: 4 sec
        cleanup();
        //then
        assertCompletedObservables("o1", "o2", "o3", "o4");          //none have expired yet

        //when
        completeObservables("o4");
        timeSource.inc(1);                //time is: 5 sec
        cleanup();
        //then
        assertCompletedObservables("o1", "o2", "o3", "o4");          //none have expired yet

        //when
        timeSource.inc(5);                //time is: 10 sec
        cleanup();
        //then
        assertCompletedObservables("o2", "o4");                      //some have expired

        //when
        timeSource.inc(3);                //time is: 13 sec
        cleanup();
        //then
        assertCompletedObservables("o4");                            //more have expired

        //when
        timeSource.inc(1);                //time is: 14 sec
        cleanup();
        //then
        assertCompletedObservables();                                //all have expired
    }

    @Test
    public void initCancelsExpiration() {
        initObservables("o1", "o2", "o3", "o4", "o5");

        //when
        completeObservables("o1", "o2", "o3", "o4");
        timeSource.inc(3);                //time is: 3 sec
        cleanup();
        //then
        assertCompletedObservables("o1", "o2", "o3", "o4");          //none have expired yet

        //when
        initObservables("o2", "o4");
        cleanup();
        //then
        assertCompletedObservables("o1", "o3");                      //expirations were cancelled
    }

    private void initObservables(String... observables) {
        repository.initObservables(Arrays.asList(observables));
    }

    private void completeObservables(String... observables) {
        repository.completeObservables(Arrays.asList(observables), null);
    }

    private void cleanup() {
        for (int i = 0; i < instances().length; i++) {
            repository.cleanup();
        }
    }

    private void assertCompletedObservables(String... observables) {
        IMap<String, Long> map = instance().getMap(ObservableRepository.COMPLETED_OBSERVABLES_MAP_NAME);
        assertEqualsEventually(() -> toSortedList(map.keySet()), toSortedList(observables));
    }

    private List<String> toSortedList(String[] array) {
        return toSortedList(Arrays.asList(array));
    }

    private List<String> toSortedList(Collection<String> collection) {
        List<String> list = new ArrayList<>(collection);
        list.sort(String::compareTo);
        return list;
    }

    private static class RoundRobinMemberSelector implements MemberSelector {

        private final Member[] members;

        private int index;
        private int count;

        RoundRobinMemberSelector(NodeEngine nodeEngine) {
            members = new ArrayList<>(nodeEngine.getClusterService().getMembers()).toArray(new Member[0]);
            index = 0;
        }

        @Override
        public boolean select(Member member) {
            boolean retVal = member == members[index];
            count++;
            if (count >= members.length) {
                count = 0;
                index++;
                if (index >= members.length) {
                    index = 0;
                }
            }
            return retVal;
        }
    }

    private static class TestTimeSource implements LongSupplier {

        private long valueMs;

        void inc(long seconds) {
            valueMs += TimeUnit.SECONDS.toMillis(seconds);
        }

        @Override
        public long getAsLong() {
            return valueMs;
        }
    }
}
