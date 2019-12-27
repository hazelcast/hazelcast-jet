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
import com.hazelcast.collection.ItemListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static com.hazelcast.jet.core.JetProperties.JOB_RESULTS_MAX_SIZE;
import static com.hazelcast.jet.core.JetProperties.JOB_RESULTS_TTL_SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ObservableRepositoryTest {

    private JetInstance jet;
    private HazelcastInstance hz;
    private TestTimeSource timeSource;
    private ObservableRepository repository;
    private TestList testList;
    private Map<String, Ringbuffer> ringbuffers;

    @Before
    public void before() {
        JetConfig config = new JetConfig();
        Properties properties = config.getProperties();
        properties.setProperty(JOB_RESULTS_TTL_SECONDS.getName(), Integer.toString(10));
        properties.setProperty(JOB_RESULTS_MAX_SIZE.getName(), Integer.toString(20));

        ringbuffers = new ConcurrentHashMap<>();

        testList = new TestList();
        hz = mock(HazelcastInstance.class);

        jet = mock(JetInstance.class);
        when(jet.getHazelcastInstance()).thenReturn(hz);

        resetHzMock();

        timeSource = new TestTimeSource();
        repository = new ObservableRepository(jet, config, timeSource);
    }

    @Test
    public void cleanup_in_batches() {
        //when
        completeObservables("o1", "o2");
        timeSource.inc(TimeUnit.SECONDS, 1);
        resetHzMock();
        repository.cleanup();
        //then
        verify(hz, never()).getRingbuffer(any());

        //when
        completeObservables("o3");
        timeSource.inc(TimeUnit.SECONDS, 1);
        resetHzMock();
        repository.cleanup();
        //then
        verify(hz, never()).getRingbuffer(any());

        //when
        completeObservables("o4", "o5", "o6", "o7", "o8", "o9", "o10", "o11", "o12", "o13", "o14", "o15", "o16", "o17",
                "o18", "o19", "o20");
        timeSource.inc(TimeUnit.SECONDS, 8);
        resetHzMock();
        repository.cleanup();
        //then
        verifyRingbuffersDestroyed("o1", "o2");
        verifyNoMoreInteractions(hz);

        //when
        resetHzMock();
        timeSource.inc(TimeUnit.SECONDS, 1);
        repository.cleanup();
        //then
        verifyRingbuffersDestroyed("o3");
        verifyNoMoreInteractions(hz);

        //when
        resetHzMock();
        timeSource.inc(TimeUnit.SECONDS, 1);
        repository.cleanup();
        //then only a fixed number of topics get clean-ed up
        verifyRingbuffersDestroyed("o4", "o5", "o6", "o7", "o8", "o9", "o10", "o11", "o12", "o13");
        verifyNoMoreInteractions(hz);

        //when
        resetHzMock();
        repository.cleanup();
        //then more topics get cleaned up
        verifyRingbuffersDestroyed("o14", "o15", "o16", "o17", "o18", "o19", "o20");
        verifyNoMoreInteractions(hz);
    }

    @Test
    public void cleanup_to_max_size() {
        //when
        completeObservables("o1", "o2");

        timeSource.inc(TimeUnit.SECONDS, 1);

        completeObservables("o3", "o4", "o5", "o6", "o7", "o8", "o9", "o10", "o11", "o12", "o13", "o14", "o15", "o16",
                "o17", "o18", "o19", "o20", "o21", "o22", "o23", "o24", "o25");

        timeSource.inc(TimeUnit.SECONDS, 9);
        resetHzMock();
        repository.cleanup(); //at this

        //then (at this time only "o1" & "o2" is expired)
        verifyRingbuffersDestroyed("o1", "o2"); //expired
        verifyRingbuffersDestroyed("o3", "o4", "o5"); //size too big
        verifyNoMoreInteractions(hz);
    }

    private void verifyRingbuffersDestroyed(String... observables) {
        for (String observable : observables) {
            String ringbufferName = "__jet.observables." + observable;

            verify(hz).getRingbuffer(ringbufferName);

            Ringbuffer ringbuffer = ringbuffers.get(ringbufferName);
            verify(ringbuffer).destroy();
        }
    }

    private void completeObservables(String... observables) {
        repository.completeObservables(Arrays.asList(observables), null);
    }

    private void resetHzMock() {
        reset(hz);

        when(hz.getList(any())).thenReturn(testList);

        doAnswer(invocation -> {
            String name = invocation.getArgument(0);
            return ringbuffers.computeIfAbsent(name, n -> mock(Ringbuffer.class));
        }).when(hz).getRingbuffer(any());
    }

    private static class TestTimeSource implements LongSupplier {

        private long valueMs;

        void inc(TimeUnit unit, long duration) {
            valueMs += unit.toMillis(duration);
        }

        @Override
        public long getAsLong() {
            return valueMs;
        }
    }

    private static class TestList<E> extends ArrayList<E> implements IList<E> {
        @Nonnull
        @Override
        public String getName() {
            return "name";
        }

        @Nonnull
        @Override
        public UUID addItemListener(@Nonnull ItemListener<E> listener, boolean includeValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeItemListener(@Nonnull UUID registrationId) {
            return false;
        }

        @Override
        public String getPartitionKey() {
            return null;
        }

        @Override
        public String getServiceName() {
            return null;
        }

        @Override
        public void destroy() {
        }
    }
}
