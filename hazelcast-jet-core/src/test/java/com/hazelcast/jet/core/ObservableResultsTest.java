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
import com.hazelcast.jet.Observer;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ObservableResultsTest extends TestInClusterSupport {

    private String observableName;
    private TestObserver testObserver;

    @Before
    public void before() {
        createJetMember();
        createJetMember();

        observableName = randomName();
        testObserver = new TestObserver();
        jet().<Long>getObservable(observableName).addObserver(testObserver);
    }

    @Test
    public void batchJobCompletesSuccessfully() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName));

        Job job = jet().newJob(pipeline);
        job.join();

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

        Job job = jet().newJob(pipeline);
        assertTrueEventually(() -> assertEquals(JobStatus.FAILED, job.getStatus()));

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

        Job job = jet().newJob(pipeline);

        assertTrueEventually(() -> assertTrue(testObserver.getSortedValues().size() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        job.cancel();
        assertTrueEventually(() -> assertTrue(testObserver.getSortedValues().size() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);
    }

    @Test
    public void multipleObservables() {
        Pipeline pipeline = Pipeline.create();
        BatchStage<Long> stage = pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L));

        TestObserver otherTestObserver = new TestObserver();
        jet().<Long>getObservable("otherObservable").addObserver(otherTestObserver);

        stage.filter(i -> i % 2 == 0).writeTo(Sinks.observable(observableName));
        stage.filter(i -> i % 2 != 0).writeTo(Sinks.observable("otherObservable"));

        Job job = jet().newJob(pipeline);
        job.join();

        assertSortedValues(testObserver, 0L, 2L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);

        assertSortedValues(otherTestObserver, 1L, 3L);
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 1);
    }

    private static void assertSortedValues(TestObserver observer, Long... values) {
        assertTrueEventually(() -> assertEquals(Arrays.asList(values), observer.getSortedValues()));
    }

    private static void assertError(TestObserver observer, String error) {
        if (error == null) {
            assertNull(observer.getError());
        } else {
            assertTrueEventually(() ->
                    assertTrue(Objects.requireNonNull(observer.getError()).getMessage().contains(error)));
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
