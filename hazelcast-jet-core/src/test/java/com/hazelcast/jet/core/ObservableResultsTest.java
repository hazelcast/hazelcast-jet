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

package com.hazelcast.jet.core;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.Observer;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ObservableResultsTest extends TestInClusterSupport {

    private String observableName;
    private TestObserver testObserver;
    private Observable<Long> testObservable;

    @Before
    public void before() {
        observableName = randomName();
        testObserver = new TestObserver();
        testObservable = jet().getObservable(observableName);
        testObservable.addObserver(testObserver);
    }

    @After
    public void after() {
        testObservable.destroy();
    }

    @Test
    public void batchJobCompletesSuccessfully() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName));

        //when
        jet().newJob(pipeline).join();
        //then
        assertSortedValues(testObserver, 0L, 1L, 2L, 3L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);
    }

    @Test
    public void batchJobFails() {
        BatchSource<String> errorSource = SourceBuilder
                .batch("error-source", x -> (Object) null)
                .<String>fillBufferFn((in, Void) -> {
                    throw new Exception("Ooops!");
                })
                .destroyFn(ConsumerEx.noop())
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(errorSource)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = jet().newJob(pipeline);
        assertTrueEventually(() -> assertEquals(JobStatus.FAILED, job.getStatus()));
        //then
        assertSortedValues(testObserver);
        assertError(testObserver, "Ooops!");
        assertCompletions(testObserver, 0);
    }

    @Test
    public void streamJob() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = jet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(testObserver.getSortedValues().size() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        //when
        job.cancel();
        //then
        assertError(testObserver, "CancellationException");
        assertCompletions(testObserver, 0);
    }

    @Test
    public void streamJobRestart() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = jet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(testObserver.getSortedValues().size() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        //when
        job.restart();
        //then
        int resultsSoFar = testObserver.getSortedValues().size();
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
        assertTrueEventually(() -> assertTrue(testObserver.getSortedValues().size() > resultsSoFar));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);
    }

    @Test
    public void multipleObservables() {
        Pipeline pipeline = Pipeline.create();
        BatchStage<Long> stage = pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L));

        TestObserver otherTestObserver = new TestObserver();
        Observable<Long> otherObservable = jet().getObservable("otherObservable");
        otherObservable.addObserver(otherTestObserver);

        stage.filter(i -> i % 2 == 0).writeTo(Sinks.observable(observableName));
        stage.filter(i -> i % 2 != 0).writeTo(Sinks.observable("otherObservable"));

        //when
        Job job = jet().newJob(pipeline);
        job.join();
        //then
        assertSortedValues(testObserver, 0L, 2L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);
        //also
        assertSortedValues(otherTestObserver, 1L, 3L);
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 1);

        otherObservable.destroy();
    }

    @Test
    public void multipleIdenticalSinks() {
        Pipeline pipeline = Pipeline.create();
        BatchStage<Long> readStage = pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L));
        readStage.writeTo(Sinks.observable(observableName));
        readStage.writeTo(Sinks.observable(observableName));

        //when
        Job job = jet().newJob(pipeline);
        job.join();
        //then
        assertSortedValues(testObserver, 0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);
    }

    @Test
    public void observersGetAllEventsStillInRingbuffer() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName));

        //when
        jet().newJob(pipeline).join();
        //then
        assertSortedValues(testObserver, 0L, 1L, 2L, 3L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);

        //when
        TestObserver otherTestObserver = new TestObserver();
        jet().<Long>getObservable(observableName).addObserver(otherTestObserver);
        //then
        assertSortedValues(otherTestObserver, 0L, 1L, 2L, 3L, 4L);
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 1);
    }

    //TODO (PR-1729): removed listener doesn't get further events

    private static void assertSortedValues(TestObserver observer, Long... values) {
        assertTrueEventually(() -> assertEquals(Arrays.asList(values), observer.getSortedValues()));
    }

    private static void assertError(TestObserver observer, String error) {
        if (error == null) {
            assertNull(observer.getError());
        } else {
            assertTrueEventually(() -> {
                assertNotNull(observer.getError());
                assertTrue(observer.getError().toString().contains(error));
            });
        }
    }

    private static void assertCompletions(TestObserver observer, int completions) {
        assertTrueEventually(() -> assertEquals(completions, observer.getNoOfCompletions()));
    }

    private static final class TestObserver implements Observer<Long> {

        private final List<Long> values = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final AtomicInteger completions = new AtomicInteger();

        @Override
        public void onNext(Long value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable throwable) {
            error.set(throwable);
        }

        @Override
        public void onComplete() {
            completions.incrementAndGet();
        }

        @Nonnull
        List<Long> getSortedValues() {
            List<Long> sortedValues;
            synchronized (values) {
                sortedValues = new ArrayList<>(values);
            }
            sortedValues.sort(Long::compare);
            return sortedValues;
        }

        @Nullable
        Throwable getError() {
            return error.get();
        }

        int getNoOfCompletions() {
            return completions.get();
        }
    }

}
