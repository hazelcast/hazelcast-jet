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

package com.hazelcast.jet;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Test;

import java.util.TreeMap;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Testing extends JetTestSupport {

    @Test
    public void test() {
        JetInstance instance = createJetMember();
        try {
            Pipeline p = Pipeline.create();
            p.readFrom(TestSources.items(IntStream.range(0, 2_000).boxed().toArray(Integer[]::new)))
             .apply(throttle(100))
             .writeTo(Sinks.noop());

            Job job = instance.newJob(p);
            long start = System.nanoTime();
            job.join();
            System.out.println("Duration: " + NANOSECONDS.toMillis(System.nanoTime() - start));
        } finally {
            instance.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    private <T, S extends GeneralStage<T>> FunctionEx<S, S> throttle(int itemsPerSecond) {
        // context for the mapUsingService stage
        class Service {
            final int ratePerSecond;
            final TreeMap<Long, Long> counts = new TreeMap<>();

            public Service(int ratePerSecond) {
                this.ratePerSecond = ratePerSecond;
            }
        }

        // factory for the service
        ServiceFactory<?, Service> serviceFactory = ServiceFactories
                .nonSharedService(procCtx ->
                        // divide the count for the actual number of processors we have
                        new Service(Math.max(1, itemsPerSecond / procCtx.totalParallelism())))
                // non-cooperative is needed because we sleep in the mapping function
                .toNonCooperative();

        return stage -> (S) stage
                .mapUsingService(serviceFactory,
                        (ctx, item) -> {
                            // current time in 10ths of a second
                            long now = System.nanoTime() / 100_000_000;
                            // include this item in the counts
                            ctx.counts.merge(now, 1L, Long::sum);
                            // clear items emitted more than second ago
                            ctx.counts.headMap(now - 10, true).clear();
                            long countInLastSecond =
                                    ctx.counts.values().stream().mapToLong(Long::longValue).sum();
                            // if we emitted too many items, sleep a while
                            if (countInLastSecond > ctx.ratePerSecond) {
                                Thread.sleep(
                                        (countInLastSecond - ctx.ratePerSecond) * 1000 / ctx.ratePerSecond);
                            }
                            // now we can pass the item on
                            return item;
                        }
                );
    }
}
