/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet2.impl.AbstractProducer;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class CancellationTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory();
        StuckProcessor.callCounter.set(0);
    }

    @Test
    public void when_jobCancelledOnSingleNode_then_shouldTerminate() throws Throwable {
        HazelcastInstance instance = factory.newHazelcastInstance();
        JetEngine jetEngine = JetEngine.get(instance, "jetEngine");

        // Given
        DAG dag = new DAG();
        Vertex slow = new Vertex("slow", StuckProcessor::new);
        dag.addVertex(slow);

        // When
        Future<Void> future = jetEngine.newJob(dag).execute();
        future.cancel(true);

        assertTaskletComplete();

        expectedException.expect(CancellationException.class);
        future.get();
    }

    @Test
    public void when_jobCancelledOnMultipleNodes_then_shouldTerminate() throws Throwable {
        factory.newHazelcastInstance();
        HazelcastInstance instance = factory.newHazelcastInstance();
        JetEngine jetEngine = JetEngine.get(instance, "jetEngine");

        // Given
        DAG dag = new DAG();
        Vertex slow = new Vertex("slow", StuckProcessor::new);
        dag.addVertex(slow);

        // When
        Future<Void> future = jetEngine.newJob(dag).execute();
        future.cancel(true);

        assertTaskletComplete();

        expectedException.expect(CancellationException.class);
        future.get();
    }

    private void assertTaskletComplete() {
        final long[] previous = {0};
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long current = StuckProcessor.callCounter.get();
                long last = previous[0];
                previous[0] = current;
                assertTrue("Call counter should eventually stop being incremented.", current == last);
            }
        });
    }

    private static class StuckProcessor extends AbstractProducer {

        private static final AtomicLong callCounter = new AtomicLong();

        @Override
        public boolean complete() {
            callCounter.incrementAndGet();
            sleepMillis(1);
            return false;
        }
    }

}
