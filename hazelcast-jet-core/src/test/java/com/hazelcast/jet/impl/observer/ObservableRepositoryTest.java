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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.topic.ITopic;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static com.hazelcast.jet.core.JetProperties.JOB_RESULTS_TTL_SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ObservableRepositoryTest {

    private JetInstance jet;
    private TestTimeSource timeSource;
    private ObservableRepository repository;

    @Before
    public void before() {
        JetConfig config = new JetConfig();
        Properties properties = config.getProperties();
        properties.setProperty(JOB_RESULTS_TTL_SECONDS.getName(), Integer.toString(10));

        jet = mock(JetInstance.class);
        resetJetMock();

        timeSource = new TestTimeSource();
        repository = new ObservableRepository(jet, config, timeSource);
    }

    @Test
    public void cleanup() {
        //when
        repository.completeObservables(Arrays.asList("o1", "o2"), null);
        timeSource.inc(TimeUnit.SECONDS, 1);
        resetJetMock();
        repository.cleanup();
        //then
        verify(jet, never()).getTopic(any());

        //when
        repository.completeObservables(Collections.singletonList("o3"), null);
        timeSource.inc(TimeUnit.SECONDS, 1);
        resetJetMock();
        repository.cleanup();
        //then
        verify(jet, never()).getTopic(any());

        //when
        repository.completeObservables(Arrays.asList("o4", "o5", "o6", "o7", "o8", "o9", "o10", "o11", "o12", "o13", "o14",
                "o15", "o16", "o17", "o18", "o19", "o20"), null);
        timeSource.inc(TimeUnit.SECONDS, 8);
        resetJetMock();
        repository.cleanup();
        //then
        verify(jet).getTopic(eq("o1"));
        verify(jet).getTopic(eq("o2"));
        verifyNoMoreInteractions(jet);

        //when
        resetJetMock();
        timeSource.inc(TimeUnit.SECONDS, 1);
        repository.cleanup();
        //then
        verify(jet).getTopic(eq("o3"));
        verifyNoMoreInteractions(jet);

        //when
        resetJetMock();
        timeSource.inc(TimeUnit.SECONDS, 1);
        repository.cleanup();
        //then only a fixed number of topics get clean-ed up
        verify(jet).getTopic(eq("o4"));
        verify(jet).getTopic(eq("o5"));
        verify(jet).getTopic(eq("o6"));
        verify(jet).getTopic(eq("o7"));
        verify(jet).getTopic(eq("o8"));
        verify(jet).getTopic(eq("o9"));
        verify(jet).getTopic(eq("o10"));
        verify(jet).getTopic(eq("o11"));
        verify(jet).getTopic(eq("o12"));
        verify(jet).getTopic(eq("o13"));
        verifyNoMoreInteractions(jet);

        //when
        resetJetMock();
        repository.cleanup();
        //then more topics get cleaned up
        verify(jet).getTopic(eq("o14"));
        verify(jet).getTopic(eq("o15"));
        verify(jet).getTopic(eq("o16"));
        verify(jet).getTopic(eq("o17"));
        verify(jet).getTopic(eq("o18"));
        verify(jet).getTopic(eq("o19"));
        verify(jet).getTopic(eq("o20"));
        verifyNoMoreInteractions(jet);
    }

    private void resetJetMock() {
        reset(jet);
        when(jet.getTopic(any())).thenReturn(mock(ITopic.class));
        when(jet.getList(any())).thenReturn(new TestList<>());
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
