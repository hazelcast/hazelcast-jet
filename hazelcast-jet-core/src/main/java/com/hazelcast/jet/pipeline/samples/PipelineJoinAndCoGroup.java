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

import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.pipeline.JoinBuilder;
import com.hazelcast.jet.pipeline.PStream;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.cogroup.CoGroupBuilder;
import com.hazelcast.jet.pipeline.tuple.TaggedTuple;
import com.hazelcast.jet.pipeline.tuple.Tuple2;
import com.hazelcast.jet.pipeline.tuple.Tuple3;
import com.hazelcast.jet.pipeline.tuple.TupleTag;

import java.util.Collection;
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
        PStream<Tuple3<Trade, Product, Broker>> joined = trades.join(
                products, onKeys(Trade::productId, Product::id),
                brokers, onKeys(Trade::brokerId, Broker::id));
    }

    private void joinBuild() {
        JoinBuilder<Trade> builder = trades.joinBuilder();
        TupleTag<Trade> tInd = builder.leftIndex();
        TupleTag<Product> pInd = builder.add(products, onKeys(Trade::productId, Product::id));
        TupleTag<Broker> bInd = builder.add(brokers, onKeys(Trade::brokerId, Broker::id));

        PStream<TaggedTuple> joined = builder.build();
        PStream<String> mapped = joined.map((TaggedTuple kt) -> {
            Trade trade = kt.get(tInd);
            Product product = kt.get(pInd);
            Broker broker = kt.get(bInd);
            return "" + trade + product + broker;
        });
    }

    private void coGroupDirect() {
        trades.coGroup(
                Trade::classId,
                products, Product::classId,
                brokers, Broker::classId,
                AggregateOperation.of(
                        StringBuilder::new,
                        (StringBuilder acc,
                         Tuple3<Collection<Trade>, Collection<Product>, Collection<Broker>> tuple) -> {
                            Collection<Trade> trades = tuple.f1();
                            Collection<Product> products = tuple.f2();
                            Collection<Broker> brokers = tuple.f3();
                            acc.append('|')
                               .append(trades)
                               .append('|')
                               .append(products)
                               .append('|')
                               .append(brokers)
                               .append('\n');
                        },
                        StringBuilder::append,
                        null,
                        StringBuilder::toString
                ));
    }

    private void coGroupBuild() {
        CoGroupBuilder<Integer, Trade> builder = trades.coGroupBuilder(Trade::classId);
        TupleTag<Collection<Trade>> tInd = builder.leftTag();
        TupleTag<Collection<Product>> pInd = builder.add(products, Product::classId);
        TupleTag<Collection<Broker>> bInd = builder.add(brokers, Broker::classId);

        PStream<Tuple2<Integer, String>> grouped = builder.build(AggregateOperation.of(
                StringBuilder::new,
                (StringBuilder acc, TaggedTuple tt) -> {
                    Collection<Trade> trades = tt.get(tInd);
                    Collection<Product> products = tt.get(pInd);
                    Collection<Broker> brokers = tt.get(bInd);
                    acc.append('|')
                       .append(trades)
                       .append('|')
                       .append(products)
                       .append('|')
                       .append(brokers)
                       .append('\n');
                },
                StringBuilder::append,
                null,
                StringBuilder::toString
        ));
        PStream<String> mapped = grouped.map(Object::toString);
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
