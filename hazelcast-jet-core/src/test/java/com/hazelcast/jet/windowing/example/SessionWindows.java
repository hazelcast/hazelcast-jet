/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.windowing.example;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.DistributedCollector;
import com.hazelcast.util.MutableLong;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.DistributedFunctions.entryKey;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLag;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.sessionWindow;
import static com.hazelcast.jet.windowing.example.SessionWindows.SourceP.eventCount;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class SessionWindows {

    private static final int MAX_SEQ_GAP = 10_000;
    private static final int SEQ_SPREAD = 8_000;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("hazelcast.logging.type", "log4j");
        ToLongFunction<Entry<String, Long>> eventSeqF = Entry::getValue;
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SourceP.metaSupplier());
        Vertex punctuation = dag.newVertex("punctuation",
                insertPunctuation(eventSeqF, () -> cappingEventSeqLag(MAX_SEQ_GAP)));
        Vertex session = dag.newVertex("session", sessionWindow(
                MAX_SEQ_GAP,
                eventSeqF,
                entryKey(),
                DistributedCollector.of(
                        MutableLong::new,
                        (acc, e) -> acc.value++,
                        (a, b) -> MutableLong.valueOf(a.value + b.value),
                        a -> a.value
                )));
        Vertex sink = dag.newVertex("sink", SinkP::new);

        dag.edge(between(source, punctuation).oneToMany())
           .edge(between(punctuation, session).oneToMany())
           .edge(between(session, sink.localParallelism(1)));
        try {
            Jet.newJetInstance();
            Jet.newJetInstance().newJob(dag).execute().get();
        } finally {
            Jet.shutdownAll();
        }
    }

    static class SourceP extends AbstractProcessor {

        static final AtomicLong eventCount = new AtomicLong();

        private final String key;

        SourceP(String key) {
            this.key = key;
        }

        static ProcessorMetaSupplier metaSupplier() {
            return addresses -> addr -> {
                String key = String.valueOf(addr.getPort());
                return count -> range(0, count).mapToObj(i -> new SourceP(key + i)).collect(toList());
            };
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            long runLength = SECONDS.toMillis(5);
            long eventSeq = 0;
            Random rnd = ThreadLocalRandom.current();
            int seqJumpProbability = 10 * MAX_SEQ_GAP;
            int seqJump = (int) (MAX_SEQ_GAP * 10);
            long start = System.currentTimeMillis();
            long localCount = 0;
            while (System.currentTimeMillis() - start < runLength) {
                tryEmit(entry(key, eventSeq++ + rnd.nextInt(SEQ_SPREAD)));
                localCount++;
                if (rnd.nextInt(seqJumpProbability) == 0) {
                    eventSeq += seqJump;
                }
            }
            eventCount.addAndGet(localCount);
            return true;
        }
    }

    static class SinkP extends AbstractProcessor {

        private long start;

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            this.start = System.nanoTime();
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
//            System.out.println(item);
            return true;
        }

        @Override
        public boolean complete() {
            System.out.format("Throughput %,d items per second%n",
                    SECONDS.toNanos(1) * eventCount.get() / (System.nanoTime() - start));
            return true;
        }
    }
}
