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

package com.hazelcast.jet.pipeline.samples;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.pipeline.CoGroupBuilder;
import com.hazelcast.jet.pipeline.JoinBuilder;
import com.hazelcast.jet.pipeline.ComputeStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.bag.BagsByTag;
import com.hazelcast.jet.pipeline.bag.Tag;
import com.hazelcast.jet.pipeline.bag.ThreeBags;
import com.hazelcast.jet.pipeline.tuple.Tuple2;
import com.hazelcast.jet.pipeline.tuple.Tuple3;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.pipeline.JoinOn.onKeys;

@SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
public class PipelineJoinAndCoGroup {

    private static final String TRADES = "trades";
    private static final String PRODUCTS = "products";
    private static final String BROKERS = "brokers";
    private static final String RESULT = "result";
    private static final String RESULT_BROKER = "result_broker";

    private final JetInstance jet;
    private Pipeline p = Pipeline.create();
    private ComputeStage<Trade> trades = p.drawFrom(Sources.<Integer, Trade>readMap(TRADES))
                                          .map(Entry::getValue);
    private ComputeStage<Product> products = p.drawFrom(Sources.<Integer, Product>readMap(PRODUCTS))
                                              .map(Entry::getValue);
    private ComputeStage<Broker> brokers = p.drawFrom(Sources.<Integer, Broker>readMap(BROKERS))
                                            .map(Entry::getValue);

    private PipelineJoinAndCoGroup(JetInstance jet) {
        this.jet = jet;
    }

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.newJetInstance();
        PipelineJoinAndCoGroup sample = new PipelineJoinAndCoGroup(jet);
        try {
            sample.prepareSampleData();
            printImap(jet.getMap(PRODUCTS));
            printImap(jet.getMap(BROKERS));
            printImap(jet.getMap(TRADES));
            sample.coGroupBuild().drainTo(Sinks.writeMap(RESULT));
            // This line added to test multiple outputs from a PElement
            sample.trades.map(t -> entry(t.brokerId, t)).drainTo(Sinks.writeMap(RESULT_BROKER));

            sample.p.execute(jet).get();

            printImap(jet.getMap(RESULT));
            printImap(jet.getMap(RESULT_BROKER));
        } finally {
            Jet.shutdownAll();
        }
    }

    public static <K, V> void printImap(IMap<K, V> imap) {
        StringBuilder sb = new StringBuilder();
        System.err.println(imap.getName() + ':');
        imap.forEach((k, v) -> sb.append(k).append("->").append(v).append('\n'));
        System.err.println(sb);
    }

    private void prepareSampleData() {
        IMap<Integer, Product> productMap = jet.getMap(PRODUCTS);
        IMap<Integer, Broker> brokerMap = jet.getMap(BROKERS);
        IMap<Integer, Trade> tradeMap = jet.getMap(TRADES);

        int productId = 21;
        int brokerId = 31;
        int tradeId = 1;
        for (int classId = 11; classId < 13; classId++) {
            for (int i = 0; i < 2; i++) {
                productMap.put(productId, new Product(classId, productId));
                brokerMap.put(brokerId, new Broker(classId, brokerId));
                tradeMap.put(tradeId++, new Trade(classId, productId, brokerId));
                productId++;
                brokerId++;
            }
        }
    }

    private ComputeStage<String> joinDirect() {
        ComputeStage<Tuple3<Trade, Iterable<Product>, Iterable<Broker>>> joined = trades.join(
                products, onKeys(Trade::productId, Product::id),
                brokers, onKeys(Trade::brokerId, Broker::id));

        return joined.map(t -> {
            Trade trade = t.f1();
            Iterable<Product> products = t.f2();
            Iterable<Broker> brokers = t.f3();
            return "" + trade + products + brokers;
        });
    }

    private ComputeStage<String> joinBuild() {
        JoinBuilder<Trade> builder = trades.joinBuilder();
        Tag<Product> productTag = builder.add(products, onKeys(Trade::productId, Product::id));
        Tag<Broker> brokerTag = builder.add(brokers, onKeys(Trade::brokerId, Broker::id));
        ComputeStage<Tuple2<Trade, BagsByTag>> joined = builder.build();

        return joined.map(t -> {
            Trade trade = t.f0();
            BagsByTag bags = t.f1();
            Iterable<Product> products = bags.bag(productTag);
            Iterable<Broker> brokers = bags.bag(brokerTag);
            return "" + trade + products + brokers;
        });
    }

    private ComputeStage<Tuple2<Integer, String>> coGroupDirect() {
        return trades.coGroup(
                Trade::classId,
                products, Product::classId,
                brokers, Broker::classId,
                AggregateOperation
                        .withCreate(ThreeBags<Trade, Product, Broker>::new)
                        .<Trade>andAccumulate0((acc, trade) -> acc.bag1().add(trade))
                        .<Product>andAccumulate1((acc, product) -> acc.bag2().add(product))
                        .<Broker>andAccumulate2((acc, broker) -> acc.bag3().add(broker))
                        .andCombine(ThreeBags::combineWith)
                        .andFinish(Object::toString));
    }

    private ComputeStage<Tuple2<Integer, String>> coGroupBuild() {
        CoGroupBuilder<Integer, Trade> builder = trades.coGroupBuilder(Trade::classId);
        Tag<Trade> tradeTag = builder.leftTag();
        Tag<Product> prodTag = builder.add(products, Product::classId);
        Tag<Broker> brokTag = builder.add(brokers, Broker::classId);

        return builder.build(AggregateOperation
                .withCreate(BagsByTag::new)
                .andAccumulate(tradeTag, (acc, trade) -> acc.bag(tradeTag).add(trade))
                .andAccumulate(prodTag, (acc, product) -> acc.bag(prodTag).add(product))
                .andAccumulate(brokTag, (acc, broker) -> acc.bag(brokTag).add(broker))
                .andCombine(BagsByTag::combineWith)
                .andFinish(Object::toString)
        );
    }

    private static class Trade implements Serializable {

        private int productId;
        private int brokerId;
        private int classId;

        Trade(int classId, int productId, int brokerId) {
            this.productId = productId;
            this.brokerId = brokerId;
            this.classId = classId;
        }

        int productId() {
            return productId;
        }

        int brokerId() {
            return brokerId;
        }

        int classId() {
            return classId;
        }

        @Override
        public String toString() {
            return "Trade{productId=" + productId + ", brokerId=" + brokerId + ", classId=" + classId + '}';
        }
    }

    private static class Product implements Serializable {

        private int id;
        private int classId;

        Product(int classId, int id) {
            this.id = id;
            this.classId = classId;
        }

        int id() {
            return id;
        }

        int classId() {
            return classId;
        }

        @Override
        public String toString() {
            return "Product{id=" + id + ", classId=" + classId + '}';
        }
    }

    private static class Broker implements Serializable {

        private int id;
        private int classId;

        Broker(int classId, int id) {
            this.id = id;
            this.classId = classId;
        }

        int id() {
            return id;
        }

        int classId() {
            return classId;
        }

        @Override
        public String toString() {
            return "Broker{id=" + id + ", classId=" + classId + '}';
        }
    }
}
