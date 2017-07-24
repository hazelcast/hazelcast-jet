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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.pipeline.CoGroupBuilder;
import com.hazelcast.jet.pipeline.JoinBuilder;
import com.hazelcast.jet.pipeline.PStream;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.bag.BagsByTag;
import com.hazelcast.jet.pipeline.bag.Tag;
import com.hazelcast.jet.pipeline.bag.ThreeBags;
import com.hazelcast.jet.pipeline.tuple.Tuple2;
import com.hazelcast.jet.pipeline.tuple.Tuple3;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map.Entry;

import static com.hazelcast.jet.pipeline.JoinOn.onKeys;

@SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
public class PipelineJoinAndCoGroup {

    private Pipeline p = Pipeline.create();
    private PStream<Trade> trades = p.drawFrom(Sources.<Integer, Trade>readMap("trades"))
                                     .map(Entry::getValue);
    private PStream<Product> products = p.drawFrom(Sources.<Integer, Product>readMap("products"))
                                         .map(Entry::getValue);
    private PStream<Broker> brokers = p.drawFrom(Sources.<Integer, Broker>readMap("brokers"))
                                       .map(Entry::getValue);


    public static void main(String[] args) {
        PipelineJoinAndCoGroup sample = new PipelineJoinAndCoGroup();
        sample.coGroupBuild().drainTo(Sinks.writeMap("map"));
        sample.p.toDag();
    }

    private PStream<String> joinDirect() {
        PStream<Tuple3<Trade, Iterable<Product>, Iterable<Broker>>> joined = trades.join(
                products, onKeys(Trade::productId, Product::id),
                brokers, onKeys(Trade::brokerId, Broker::id));

        return joined.map(t -> {
            Trade trade = t.f1();
            Iterable<Product> products = t.f2();
            Iterable<Broker> brokers = t.f3();
            return "" + trade + products + brokers;
        });
    }

    private PStream<String> joinBuild() {
        JoinBuilder<Trade> builder = trades.joinBuilder();
        Tag<Product> productTag = builder.add(products, onKeys(Trade::productId, Product::id));
        Tag<Broker> brokerTag = builder.add(brokers, onKeys(Trade::brokerId, Broker::id));
        PStream<Tuple2<Trade, BagsByTag>> joined = builder.build();

        return joined.map(t -> {
            Trade trade = t.f0();
            BagsByTag bags = t.f1();
            Iterable<Product> products = bags.bag(productTag);
            Iterable<Broker> brokers = bags.bag(brokerTag);
            return "" + trade + products + brokers;
        });
    }

    private PStream<Tuple2<Integer, String>> coGroupDirect() {
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

    private PStream<Tuple2<Integer, String>> coGroupBuild() {
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

    private static class Trade {

        private int productId;
        private int brokerId;
        private int classId;

        int productId() {
            return productId;
        }

        int brokerId() {
            return brokerId;
        }

        int classId() {
            return classId;
        }
    }

    private static class Product {

        private int id;
        private int classId;

        int id() {
            return id;
        }

        int classId() {
            return classId;
        }
    }

    private static class Broker {

        private int id;
        private int classId;

        int id() {
            return id;
        }

        int classId() {
            return classId;
        }
    }
}
