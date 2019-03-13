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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.Processor.Context;
import org.junit.Test;

import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static org.apache.activemq.transport.amqp.AmqpWireFormat.DEFAULT_IDLE_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamSourceTest extends PipelineTestSupport {

    @Test
    public void test_defaultIdle() {
        test(DEFAULT_IDLE_TIMEOUT);
    }

    @Test
    public void test_shortIdle() {
        test(1000);
    }

    private void test(int idleTimeout) {
        Pipeline p = Pipeline.create();

        StreamSource<Object> source = SourceBuilder.timestampedStream("src", Context::globalProcessorIndex)
                                                   .distributed(1)
                                                   .fillBufferFn((index, buf) -> {
                                                       if (index == 0) {
                                                           buf.add("item", System.currentTimeMillis());
                                                           Thread.sleep(10);
                                                       }
                                                   })
                                                   .build();
        if (idleTimeout != DEFAULT_IDLE_TIMEOUT) {
            source.setPartitionIdleTimeout(idleTimeout);
        }

        p.drawFrom(source)
         .withNativeTimestamps(0)
         .window(WindowDefinition.tumbling(100))
         .aggregate(counting())
         .drainTo(sink);

        Job job = allJetInstances()[0].newJob(p);

        if (idleTimeout > 10_000) {
            assertTrueAllTheTime(() -> assertEquals("unexpected sink size", 0, sinkList.size()), 5);
        } else if (idleTimeout < 1000) {
            assertTrueEventually(() -> assertTrue("sink empty", sinkList.size() > 0));
        } else {
            fail("idleTimeout=" + idleTimeout);
        }

        job.cancel();
        try {
            job.join();
        } catch (CancellationException ignored) {
        }
    }
}
