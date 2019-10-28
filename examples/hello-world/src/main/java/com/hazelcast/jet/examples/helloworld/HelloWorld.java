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

package com.hazelcast.jet.examples.helloworld;

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.map.IMap;

import java.nio.channels.Pipe;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.Util.entry;

/**
 * Demonstrates a simple Word Count job in the Pipeline API. Inserts the
 * text of The Complete Works of William Shakespeare into a Hazelcast
 * IMap, then lets Jet count the words in it and write its findings to
 * another IMap. The example looks at Jet's output and prints the 100 most
 * frequent words.
 */
public class HelloWorld {

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.drawFrom(TestSources.itemStream(10, (ts, seq) -> nextRandomNumber()))
         .withIngestionTimestamps()
         .rollingAggregate(AggregateOperations.topN(10, ComparatorEx.comparingLong(l -> l)))
         .map(l -> entry("top10", l))
         .drainTo(Sinks.map("top10"));
        return p;
    }

    private static long nextRandomNumber() {
        return ThreadLocalRandom.current().nextLong();
    }

    public static void main(String[] args) throws InterruptedException {
        JetInstance jet = JetBootstrap.getInstance();

        Pipeline p = buildPipeline();
        Job job = jet.newJob(p);

        while (true) {
            IMap<String, List<Long>> top10Map = jet.getMap("top10");

            List<Long> top10numbers = top10Map.get("top10");
            if (top10numbers != null) {
                System.out.println("Top 10 numbers are: ");
                top10numbers.forEach(n -> {
                    System.out.println(n);
                });
            }
            Thread.sleep(1000);
        }
    }

}
