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

package com.hazelcast.jet.s3;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@Category(NightlyTest.class)
public class S3SourceTest extends JetTestSupport {

    private static final String BUCKET_NAME = "jet-s3-connector-test-bucket-source";


    @Test
    public void test() {
        Sink<Object> counterSink = SinkBuilder
                .sinkBuilder("", c -> new Counter(c, "a"))
                .receiveFn(Counter::increment)
                .destroyFn(Counter::close)
                .build();

        JetInstance instance1 = createJetMember();
        JetInstance instance2 = createJetMember();

        Pipeline p = Pipeline.create();
        p.drawFrom(S3Sources.s3(singletonList(BUCKET_NAME), null, S3SinkTest::client))
         .drainTo(counterSink);

        instance1.newJob(p).join();

        IAtomicLong a = instance1.getHazelcastInstance().getAtomicLong("a");
        assertEquals(1_100_000, a.get());

    }

    private static class Counter {

        final IAtomicLong atomicLong;
        long counter;

        Counter(Processor.Context c, String a) {
            this.atomicLong = c.jetInstance().getHazelcastInstance().getAtomicLong(a);
        }

        void increment(Object item) {
            counter++;
        }

        void close() {
            atomicLong.addAndGet(counter);
        }

    }
}
