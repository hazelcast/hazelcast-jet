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
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.function.Observer;
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
import java.util.UUID;
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
    private UUID registrationId;

    @Before
    public void before() {
        observableName = randomName();
        testObserver = new TestObserver();
        testObservable = jet().getObservable(observableName);
        registrationId = testObservable.addObserver(testObserver);
    }

    @After
    public void after() {
        testObservable.destroy();
    }

    @Test
    public void iterable() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName));

        //when
        jet().newJob(pipeline).join();
        //then
        List<Long> items = new ArrayList<>();
        for (Long item : testObservable) {
            items.add(item);
        }
        items.sort(Long::compareTo);
        assertEquals(Arrays.asList(0L, 1L, 2L, 3L, 4L), items);
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
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
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
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        //when
        job.restart();
        //then
        int resultsSoFar = testObserver.getNoOfValues();
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > resultsSoFar));
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
    public void multipleJobsWithTheSameSink() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .filter(t -> (t % 2 == 0))
                .writeTo(Sinks.observable(observableName));
        Pipeline pipeline2 = Pipeline.create();
        pipeline2.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .filter(t -> (t % 2 != 0))
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = jet().newJob(pipeline);
        Job job2 = jet().newJob(pipeline2);
        //then
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job2.getStatus()));
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
        assertTrueEventually(() -> {
            List<Long> sortedValues = testObserver.getSortedValues();
            assertEquals(0, (long) sortedValues.get(0));
            assertEquals(1, (long) sortedValues.get(1));
        });
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        job.cancel();
        job2.cancel();
    }

    @Test
    public void multipleJobExecutions() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName));

        //when
        jet().newJob(pipeline).join();
        jet().newJob(pipeline).join();
        //then
        assertSortedValues(testObserver, 0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 2);
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

    @Test
    public void observableRegisteredAfterJobFinishedGetAllEventsStillInRingbuffer() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName + "late"));

        //when
        jet().newJob(pipeline).join();
        TestObserver otherTestObserver = new TestObserver();
        Observable<Long> lateObservable = jet().<Long>getObservable(observableName + "late");
        try {
            lateObservable.addObserver(otherTestObserver);
            //then
            assertSortedValues(otherTestObserver, 0L, 1L, 2L, 3L, 4L);
            assertError(otherTestObserver, null);
            assertCompletions(otherTestObserver, 1);
        } finally {
            lateObservable.destroy();
        }
    }

    @Test
    public void observableRegisteredAfterJobFailedGetError() {
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

        Job job = jet().newJob(pipeline);
        assertTrueEventually(() -> assertEquals(JobStatus.FAILED, job.getStatus()));

        //when
        TestObserver otherTestObserver = new TestObserver();
        Observable<Long> lateObservable = jet().<Long>getObservable(observableName);
        try {
            lateObservable.addObserver(otherTestObserver);
            //then
            assertSortedValues(testObserver);
            assertError(testObserver, "Ooops!");
            assertCompletions(testObserver, 0);
        } finally {
            lateObservable.destroy();
        }
    }

    @Test
    public void errorInOneJobIsNotTerminalForOthers() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));
        Pipeline pipeline2 = Pipeline.create();
        pipeline2.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = jet().newJob(pipeline);
        Job job2 = jet().newJob(pipeline2);
        //then
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job2.getStatus()));
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        //when
        job.cancel();
        assertError(testObserver, "CancellationException");
        assertCompletions(testObserver, 0);

        //then - job2 is still running
        int resultsSoFar = testObserver.getNoOfValues();
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > resultsSoFar));

        job2.cancel();
    }

    @Test
    public void veryFastPublishRate() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100_000))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = jet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 100_000));
        assertError(testObserver, null);

        job.cancel();
    }

    @Test
    public void removedObserverDoesNotGetFurtherEvents() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = jet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        //when
        testObservable.removeObserver(registrationId);
        //then
        int resultsSoFar = testObserver.getNoOfValues();
        assertTrueAllTheTime(() -> assertEquals(resultsSoFar, testObserver.getNoOfValues()), 2);

        job.cancel();
    }

    @Test
    public void destroyedObservableDoesNotGetFurtherEvents() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName + "destroyed"));

        TestObserver otherTestObserver = new TestObserver();
        Observable<Long> destroyedObservable = jet().<Long>getObservable(observableName + "destroyed");
        destroyedObservable.addObserver(otherTestObserver);
        //when
        Job job = jet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(otherTestObserver.getNoOfValues() > 10));
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 0);

        //when
        destroyedObservable.destroy();
        //then
        int resultsSoFar = otherTestObserver.getNoOfValues();
        assertTrueAllTheTime(() -> assertEquals(resultsSoFar, otherTestObserver.getNoOfValues()), 2);
        job.cancel();
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 0);
    }

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
        int getNoOfValues() {
            synchronized (values) {
                return values.size();
            }
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
