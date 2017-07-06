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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.pipeline.tuple.Tuple2;
import com.hazelcast.jet.pipeline.tuple.Tuple3;

import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.AggregateOperations.toList;
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
        TupleIndex<Trade> tInd = builder.leftIndex();
        TupleIndex<Product> pInd = builder.add(products, onKeys(Trade::productId, Product::id));
        TupleIndex<Broker> bInd = builder.add(brokers, onKeys(Trade::brokerId, Broker::id));

        PStream<KeyedTuple> joined = builder.build();
        PStream<String> mapped = joined.map((KeyedTuple kt) -> {
            Trade trade = kt.get(tInd);
            Product product = kt.get(pInd);
            Broker broker = kt.get(bInd);
            return "" + trade + product + broker;
        });
    }

    private void coGroupDirect() {
        PStream<List<Tuple3<Trade, Product, Broker>>> grouped = trades.coGroup(
                Trade::classId,
                products, Product::classId,
                brokers, Broker::classId,
                toList());
    }

    private void coGroupBuild() {
        CoGroupBuilder<Integer, Trade> builder = trades.coGroupBuilder(Trade::classId);
        TupleIndex<Trade> tInd = builder.leftIndex();
        TupleIndex<Product> pInd = builder.add(products, Product::classId);
        TupleIndex<Broker> bInd = builder.add(brokers, Broker::classId);

        PStream<KeyedTuple> grouped = builder.build();
        PStream<String> mapped = grouped.map((KeyedTuple kt) -> {
            Trade trade = kt.get(tInd);
            Product product = kt.get(pInd);
            Broker broker = kt.get(bInd);
            return "" + trade + product + broker;
        });

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
