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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.JetTestSupport.assertJobStatusEventually;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

public class ExactlyOnceSinkTestUtil {

    public static void test_transactional_withRestarts(
            @Nonnull JetInstance instance,
            @Nonnull ILogger logger,
            @Nonnull Sink<Integer> sink,
            boolean graceful, @Nonnull SupplierEx<Set<Integer>> actualItemsSupplier
    ) {
        int numItems = 1000;
        Pipeline p = Pipeline.create();
        p.readFrom(SourceBuilder.stream("src", procCtx -> new int[1])
                .<Integer>fillBufferFn((ctx, buf) -> {
                    if (ctx[0] < numItems) {
                        buf.add(ctx[0]++);
                        sleepMillis(5);
                    }
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build())
         .withoutTimestamps()
         .peek()
         .writeTo(sink);

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        JobProxy job = (JobProxy) instance.newJob(p, config);

        long endTime = System.nanoTime() + SECONDS.toNanos(60);
        int lastCount = 0;
        String expectedRows = IntStream.range(0, numItems).mapToObj(Integer::toString).collect(joining("\n"));
        // We'll restart once, then restart again after a short sleep (possibly during initialization), then restart
        // again and then assert some output so that the test isn't constantly restarting without any progress
        for (;;) {
            assertJobStatusEventually(job, RUNNING);
            job.restart(graceful);
            assertJobStatusEventually(job, RUNNING);
            sleepMillis(ThreadLocalRandom.current().nextInt(400));
            job.restart(graceful);
            try {
                Set<Integer> actualItems;
                do {
                    actualItems = actualItemsSupplier.get();
                } while (actualItems.size() < Math.min(numItems, 100 + lastCount)
                        && System.nanoTime() < endTime);
                lastCount = actualItems.size();
                logger.info("number of committed items in the sink so far: " + lastCount);
                assertEquals(expectedRows, actualItems.stream().map(Objects::toString).collect(joining("\n")));
                // if content matches, break the loop. Otherwise restart and try again
                break;
            } catch (AssertionError e) {
                if (System.nanoTime() >= endTime) {
                    throw e;
                }
            }
        }
    }
}
