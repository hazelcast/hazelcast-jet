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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.metrics.Counter;
import com.hazelcast.jet.core.metrics.MetricsContext;
import com.hazelcast.jet.core.metrics.MetricsProvider;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.Functions.wholeItem;
import static java.util.Arrays.asList;

public class LogDebug {
    static void s1() {
        //tag::s1[]
        System.setProperty("hazelcast.logging.type", "log4j");
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        JetConfig config = new JetConfig();
        config.getHazelcastConfig()
                .setProperty("hazelcast.logging.type", "log4j");
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String>list("inputList"))
                .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
                .filter(word -> !word.isEmpty())
                .peek() // <1>
                .groupingKey(wholeItem())
                .aggregate(counting())
                .drainTo(Sinks.map("counts"));
        //end::s3[]

        //tag::s4[]
        JetInstance jet = Jet.newJetInstance();
        try {
            jet.getList("inputList")
                    .addAll(asList("The quick brown fox", "jumped over the lazy dog"));
            jet.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
        //end::s4[]
    }

    static void s5() {
        BatchSource<Long> source = TestSources.items(0L, 1L, 2L, 3L, 4L);
        Sink<Long> sink = Sinks.logger();
        Pipeline pipeline = Pipeline.create();

        //tag::s5[]
        class Filter implements PredicateEx<Long>, MetricsProvider {

            private Counter dropped;
            private AtomicLong total;

            @Override
            public boolean testEx(Long l) throws Exception {
                boolean even = l % 2L == 0;
                if (!even) {
                    dropped.increment();
                }
                total.incrementAndGet();
                return even;
            }

            @Override
            public void registerMetrics(MetricsContext context) {
                dropped = context.registerCounter("dropped");

                total = new AtomicLong();
                context.registerGauge("total", total::get);
            }
        }

        pipeline.drawFrom(source)
                .filter(new Filter())
                .drainTo(sink);
        //end::s5[]

        JetInstance jet = Jet.newJetInstance();
        Job job = jet.newJob(pipeline);
        job.join();
    }

    static void s6() {
        //tag::s6[]
        //end::s6[]
    }

    static void s7() {
        //tag::s7[]
        //end::s7[]
    }

    static void s8() {
        //tag::s8[]
        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        //end::s9[]
    }

    static void s10() {
        //tag::s10[]
        //end::s10[]
    }

    static void s11() {
        //tag::s11[]
        //end::s11[]
    }

}
