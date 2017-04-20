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
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.WindowDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLagAndLull;
import static com.hazelcast.jet.windowing.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowingProcessors.groupByFrame;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;
import static java.lang.Runtime.getRuntime;

public class TradeMonitor {

    private static final Map<String, Integer> TICKERS = new HashMap<String, Integer>() {{
        put("GOOG", 10000);
        put("FB", 15000);
        put("ATVI", 15000);
        put("ADBE", 15000);
        put("AKAM", 15000);
        put("ALXN", 15000);
        put("AMZN", 15000);
        put("AAL", 15000);
        put("AMGN", 15000);
        put("ADI", 15000);
        put("AAPL", 15000);
    }};

    private static final boolean IS_SLOW = false;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        Class<TradeMonitor> thisClass = TradeMonitor.class;
        final ILogger logger = Logger.getLogger(thisClass);

        try {
            JetConfig cfg = new JetConfig();

            final int defaultLocalParallelism = Math.max(1, getRuntime().availableProcessors() / 2);
            cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(defaultLocalParallelism));

            if (!IS_SLOW) {
                Jet.newJetInstance(cfg);
            }
            JetInstance jet = Jet.newJetInstance(cfg);

            IStreamMap<String, Integer> initial = jet.getMap("initial");
            if (IS_SLOW) {
                initial.putAll(TICKERS);
            } else {
                Stream<String> lines = Files.lines(Paths.get(thisClass.getResource("/nasdaqlisted.txt").toURI()));
                lines.skip(1).map(l -> l.split("\\|")[0]).forEach(t -> initial.put(t, 10000));
            }

            WindowDefinition windowDef = slidingWindowDef(3_000, 1_000);

            DAG dag = new DAG();
            Vertex tickerSource = dag.newVertex("ticker-source", readMap(initial.getName()));
            Vertex generateEvents = slow(dag.newVertex("generate-events", () -> new GenerateTradesP(IS_SLOW ? 500 : 0)));
            Vertex insertPunctuation = slow(dag.newVertex("insert-punctuation",
                    insertPunctuation(Trade::getTime,
                            () -> cappingEventSeqLagAndLull(3000, 2000).throttleByFrame(windowDef))));
//            Vertex peek = dag.newVertex("peek", PeekP::new).localParallelism(1);
            Vertex groupByFrame = slow(dag.newVertex("group-by-frame",
                    groupByFrame(Trade::getTicker, Trade::getTime, windowDef, counting())
            ));
            Vertex slidingWindow = slow(dag.newVertex("sliding-window",
                    slidingWindow(windowDef, counting(), false)));
            Vertex sink = dag.newVertex("sink", Processors.writeMap("sink")).localParallelism(1);

            dag.edge(between(tickerSource, generateEvents).broadcast().distributed())
               .edge(between(generateEvents, insertPunctuation).oneToMany())
               .edge(between(insertPunctuation, groupByFrame).partitioned(Trade::getTicker, HASH_CODE))
               .edge(between(groupByFrame, slidingWindow).partitioned(Frame<Object, Object>::getKey)
                                                         .distributed())
               .edge(between(slidingWindow, sink));

//            dag.edge(from(generateEvents, 1).to(peek));
//            dag.edge(from(groupByFrame, 1).to(peek, 0));
//            dag.edge(from(slidingWindow, 1).to(peek, 0));

            jet.newJob(dag).execute();

            while (true) {
                logger.info("Trade count: " + GenerateTradesP.tradeCount);
                GenerateTradesP.tradeCount.set(0);
                Thread.sleep(1000);
            }

        } finally {
            Jet.shutdownAll();
        }
    }

    private static Vertex slow(Vertex v) {
        return v.localParallelism(IS_SLOW ? 1 : -1);
    }

    static class PeekP extends AbstractProcessor {
        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            getLogger().info(item.toString());
            return true;
        }
    }

}
