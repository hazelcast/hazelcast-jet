/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.JetTestSupport.assertJobStatusEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ObservableShutdownTest {

    private static final int MEMBER_COUNT = 3;

    private JetTestInstanceFactory factory;

    private JetInstance[] members;
    private JetInstance client;

    private Observable<Long> memberObservable;
    private Observable<Long> clientObservable;

    private TestObserver memberObserver;
    private TestObserver clientObserver;

    @Before
    public void before() {
        factory = new JetTestInstanceFactory();
        members = Stream.generate(factory::newMember).limit(MEMBER_COUNT).toArray(JetInstance[]::new);
        client = factory.newClient();

        memberObserver = new TestObserver();
        memberObservable = members[members.length - 1].newObservable();
        memberObservable.addObserver(memberObserver);

        clientObserver = new TestObserver();
        clientObservable = client.newObservable();
        clientObservable.addObserver(clientObserver);
    }

    @After
    public void after() throws Exception {
        spawn(() -> factory.terminateAll())
                .get(1, TimeUnit.MINUTES);
    }

    @Test
    public void cleanup() {
        Pipeline pipeline = Pipeline.create();
        StreamStage<Long> stage = pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence);

        stage.writeTo(Sinks.observable(clientObservable));
        stage.writeTo(Sinks.observable(memberObservable));

        //when
        Job job = client.newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(clientObserver.getNoOfValues() > 10));
        assertTrueEventually(() -> assertTrue(memberObserver.getNoOfValues() > 10));

        //when
        client.shutdown();
        //then
        assertObserverStopsReceivingValues(clientObserver);

        //when
        long jobId = job.getId();
        members[members.length - 1].shutdown();
        //then
        assertJobStatusEventually(members[0].getJob(jobId), JobStatus.RUNNING);
        assertObserverStopsReceivingValues(memberObserver);
    }

    private void assertObserverStopsReceivingValues(TestObserver observer) {
        assertTrueEventually(() -> {
            int values1 = observer.getNoOfValues();
            MILLISECONDS.sleep(1000);
            int values2 = observer.getNoOfValues();
            assertEquals(values1, values2);
        });
    }

    private static final class TestObserver implements Observer<Long> {

        private final AtomicInteger values = new AtomicInteger();

        @Override
        public void onNext(@Nonnull Long value) {
            values.incrementAndGet();
        }

        @Override
        public void onError(@Nonnull Throwable throwable) {
            fail("Errors aren't expected: " + throwable.getMessage());
        }

        @Override
        public void onComplete() {
            fail("Completions aren't expected");
        }

        int getNoOfValues() {
            return values.get();
        }
    }

}
