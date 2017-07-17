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

import com.hazelcast.jet.pipeline.JoinBuilder;
import com.hazelcast.jet.pipeline.PStream;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.bag.Tag;
import com.hazelcast.jet.pipeline.bag.BagsByTag;
import com.hazelcast.jet.pipeline.bag.ThreeBags;
import com.hazelcast.jet.pipeline.CoGroupBuilder;
import com.hazelcast.jet.pipeline.GroupAggregation;
import com.hazelcast.jet.pipeline.tuple.Tuple2;

import java.util.Map.Entry;

import static com.hazelcast.jet.pipeline.JoinOn.onKeys;

public class PipelineJoinAndCoGroup {

    private Pipeline p = Pipeline.create();
    private PStream<Trade> trades = p.drawFrom(Sources.<Integer, Trade>readMap("trades"))
                                     .map(Entry::getValue);
    private PStream<Product> products = p.drawFrom(Sources.<Integer, Product>readMap("products"))
                                         .map(Entry::getValue);
    private PStream<Broker> brokers = p.drawFrom(Sources.<Integer, Broker>readMap("brokers"))
                                       .map(Entry::getValue);

    private void joinDirect() {
        PStream<ThreeBags<Trade, Product, Broker>> joined = trades.join(
                products, onKeys(Trade::productId, Product::id),
                brokers, onKeys(Trade::brokerId, Broker::id));

        PStream<String> mapped = joined.map(bags -> {
            Iterable<Trade> trades = bags.bag1();
            Iterable<Product> products = bags.bag2();
            Iterable<Broker> brokers = bags.bag3();
            return "" + trades + products + brokers;
        });
    }

    private void joinBuild() {
        JoinBuilder<Trade> builder = trades.joinBuilder();
        Tag<Trade> tInd = builder.leftTag();
        Tag<Product> pInd = builder.add(products, onKeys(Trade::productId, Product::id));
        Tag<Broker> bInd = builder.add(brokers, onKeys(Trade::brokerId, Broker::id));
        PStream<BagsByTag> joined = builder.build();

        PStream<String> mapped = joined.map(bags -> {
            Iterable<Trade> trades = bags.get(tInd);
            Iterable<Product> products = bags.get(pInd);
            Iterable<Broker> brokers = bags.get(bInd);
            return "" + trades + products + brokers;
        });
    }

    private void coGroupDirect() {
        PStream<Tuple2<Integer, String>> grouped = trades.coGroup(
                Trade::classId,
                products, Product::classId,
                brokers, Broker::classId,
                GroupAggregation.of3(
                        (ThreeBags<Trade, Product, Broker> bags) ->
                                new StringBuilder()
                                        .append(bags.bag1())
                                        .append(bags.bag2())
                                        .append(bags.bag3()),
                        StringBuilder::append,
                        null,
                        StringBuilder::toString
                ));
    }

    private void coGroupBuild() {
        CoGroupBuilder<Integer, Trade> builder = trades.coGroupBuilder(Trade::classId);
        Tag<Trade> tradeTag = builder.leftTag();
        Tag<Product> prodTag = builder.add(products, Product::classId);
        Tag<Broker> brokTag = builder.add(brokers, Broker::classId);

        PStream<Tuple2<Integer, String>> grouped = builder.build(GroupAggregation.ofMany(
                bags -> {
                    StringBuilder acc = new StringBuilder();
                    Iterable<Trade> trades = bags.get(tradeTag);
                    Iterable<Product> products = bags.get(prodTag);
                    Iterable<Broker> brokers = bags.get(brokTag);
                    return acc.append(trades)
                              .append(products)
                              .append(brokers);
                },
                StringBuilder::append,
                null,
                StringBuilder::toString
        ));
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
