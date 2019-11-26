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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observer;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class ObservableResultsTest extends JetTestSupport {

    private String observableName;
    private JetInstance instance;
    private TestObserver testObserver;

    @Before
    public void before() {
        instance = createJetMember();
        createJetMember();
        createJetMember();

        observableName = randomName();
        testObserver = new TestObserver();
        instance.<Integer>getObservable(observableName).addObserver(testObserver);
    }

    @Test
    public void batchJobCompletesSuccessfully() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0, 1, 2, 3, 4))
                .writeTo(Sinks.observable(observableName));

        Job job = instance.newJob(pipeline);
        job.join();

        testObserver.assertSortedValues(Arrays.asList(0, 1, 2, 3, 4));
        testObserver.assertNoErrors();
        testObserver.assertCompletions(1);
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

        Job job = instance.newJob(pipeline);
        assertTrueEventually(() -> assertEquals(JobStatus.FAILED, job.getStatus()));

        testObserver.assertSortedValues(Collections.emptyList());
        testObserver.assertError("Ooops!");
        testObserver.assertCompletions(0);
    }

    @Test
    public void multipleObservables() {
        Pipeline pipeline = Pipeline.create();
        BatchStage<Integer> stage = pipeline.readFrom(TestSources.items(0, 1, 2, 3, 4));

        TestObserver otherTestObserver = new TestObserver();
        instance.<Integer>getObservable("otherObservable").addObserver(otherTestObserver);

        stage.filter(i -> i % 2 == 0).writeTo(Sinks.observable(observableName));
        stage.filter(i -> i % 2 != 0).writeTo(Sinks.observable("otherObservable"));

        Job job = instance.newJob(pipeline);
        job.join();

        testObserver.assertSortedValues(Arrays.asList(0, 2, 4));
        testObserver.assertNoErrors();
        testObserver.assertCompletions(1);

        otherTestObserver.assertSortedValues(Arrays.asList(1, 3));
        otherTestObserver.assertNoErrors();
        otherTestObserver.assertCompletions(1);
    }

    //todo: streaming job tests

    private static final class TestObserver implements Observer<Integer> {

        private final List<Integer> values = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final AtomicInteger completions = new AtomicInteger();

        @Override
        public void onNext(Integer integer) {
            values.add(integer);
        }

        @Override
        public void onError(Throwable throwable) {
            error.set(throwable);
        }

        @Override
        public void onComplete() {
            completions.incrementAndGet();
        }

        void assertSortedValues(List<Integer> expected) {
            List<Integer> sortedActualValues;
            synchronized (values) {
                sortedActualValues = new ArrayList<>(values);
            }
            sortedActualValues.sort(Integer::compare);

            assertEquals(expected, sortedActualValues);
        }

        void assertNoErrors() {
            assertNull(error.get());
        }

        void assertError(String errorMessage) {
            assertTrue(error.get().getMessage().contains(errorMessage));
        }

        void assertCompletions(int expectedNoOfCompletions) {
            assertEquals(expectedNoOfCompletions, completions.get());
        }
    }

}
