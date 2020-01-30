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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.examples.enrichment.datamodel.PageVisit;
import com.hazelcast.jet.examples.enrichment.datamodel.Payment;
import com.hazelcast.jet.examples.enrichment.datamodel.StockInfo;
import com.hazelcast.jet.examples.enrichment.datamodel.Trade;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.function.ComparatorEx.comparing;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sources.list;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.MINUTES;

public class CheatSheet {
    private static Pipeline p;

    static void s1() {
        //tag::s1[]
        BatchStage<String> lines = p.readFrom(list("lines"));
        BatchStage<String> lowercased = lines.map(line -> line.toLowerCase());
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        BatchStage<String> lines = p.readFrom(list("lines"));
        BatchStage<String> nonEmpty = lines.filter(line -> !line.isEmpty());
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        BatchStage<String> lines = p.readFrom(list("lines"));
        BatchStage<String> words = lines.flatMap(
                line -> traverseArray(line.split("\\W+")));
        //end::s3[]
    }

    static void s4() {
        //tag::s4[]
        BatchStage<Trade> trades = p.readFrom(list("trades"));
        BatchStage<Entry<String, StockInfo>> stockInfo =
                p.readFrom(list("stockInfo"));
        BatchStage<Trade> joined = trades.hashJoin(stockInfo,
                joinMapEntries(Trade::ticker), Trade::setStockInfo);
        //end::s4[]
    }

    static void s4a() {
        JetInstance jet = Jet.newJetInstance();
        //tag::s4a[]
        IMap<String, StockInfo> stockMap = jet.getMap("stock-info");
        StreamSource<Trade> tradesSource = tradesSource();

        p.readFrom(tradesSource)
         .withoutTimestamps()
         .groupingKey(Trade::ticker)
         .mapUsingIMap(stockMap, Trade::setStockInfo)
         .writeTo(Sinks.list("result"));
        //end::s4a[]
    }

    static void s5() {
        //tag::s5[]
        BatchStage<String> lines = p.readFrom(list("lines"));
        BatchStage<Long> count = lines.aggregate(counting());
        //end::s5[]
    }

    static void s6() {
        //tag::s6[]
        BatchStage<String> words = p.readFrom(list("words"));
        BatchStage<Entry<String, Long>> wordsAndCounts =
            words.groupingKey(word -> word)
                 .aggregate(counting());
        //end::s6[]
    }

    static void s7() {
        //tag::s7[]
        StreamStage<Tweet> tweets = tweetStream();

        tweets
            .flatMap(tweet -> Traversers.traverseArray(tweet.hashtags()))
            .groupingKey(hashtag -> hashtag)
            .window(sliding(1000, 10))
            .aggregate(counting());
        //end::s7[]
    }

    private static StreamStage<Tweet> tweetStream() {
        return p.readFrom(TestSources.itemStream(10, (x, y) -> new Tweet()))
                .withoutTimestamps();
    }

    static void s8() {
        //tag::s8[]
        BatchStage<PageVisit> pageVisits = p.readFrom(Sources.list("pageVisit"));
        BatchStage<Payment> payments = p.readFrom(Sources.list("payment"));

        BatchStageWithKey<PageVisit, Integer> pageVisitsByUserId =
                pageVisits.groupingKey(pageVisit -> pageVisit.userId());

        BatchStageWithKey<Payment, Integer> paymentsByUserId =
                payments.groupingKey(payment -> payment.userId());

        pageVisitsByUserId.aggregate2(toList(), paymentsByUserId, toList());
        // the output will two lists: one containing
        // the payments and the other the page visits, both for the
        // same user

        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        StreamStage<PageVisit> pageVisits = pageVisitsStream();
        StreamStage<Payment> payments = paymentsStream();

        StreamStageWithKey<Payment, Integer> paymentsByUserId =
            payments.groupingKey(payment -> payment.userId());

        pageVisits.groupingKey(pageVisit -> pageVisit.userId())
                  .window(sliding(60_000, 1000))
                  .aggregate2(toList(), paymentsByUserId, toList());

        // the output will be a window with two lists: one containing
        // the payments and the other the page visits, both for the
        // same user
        //end::s9[]
    }

    private static StreamStage<Payment> paymentsStream() {
        return p.<Payment>readFrom(Sources.mapJournal("payments",
                START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
            .withTimestamps(Payment::timestamp, 1000);
    }

    private static StreamStage<PageVisit> pageVisitsStream() {
        return p.<PageVisit>readFrom(
            Sources.mapJournal("pageVisits", START_FROM_OLDEST, mapEventNewValue(), mapPutEvents())
        ).withTimestamps(PageVisit::timestamp, 1000);
    }

    static void s10() {
        //tag::s10[]
        StreamSource<Trade> tradesSource = tradesSource();
        StreamStage<Trade> currLargestTrade =
            p.readFrom(tradesSource)
             .withoutTimestamps()
             .rollingAggregate(maxBy(comparing(Trade::worth)));
        //end::s10[]
    }

    private static StreamSource<Trade> tradesSource() {
        return Sources.mapJournal("tradeStream",
                START_FROM_CURRENT, mapEventNewValue(), mapPutEvents());
    }

    static void s11() {
        //tag::s11[]
        BatchStage<String> strings = someStrings();
        BatchStage<String> distinctStrings = strings.distinct();
        BatchStage<String> distinctByPrefix =
                strings.groupingKey(s -> s.substring(0, 4)).distinct();
        //end::s11[]
    }

    private static BatchStage<String> someStrings() {
        throw new UnsupportedOperationException();
    }

    static void s12() {
        //tag::s12[]
        StreamStage<Trade> tradesNewYork = tradeStream("new-york");
        StreamStage<Trade> tradesTokyo = tradeStream("tokyo");
        StreamStage<Trade> tradesNyAndTokyo =
                tradesNewYork.merge(tradesTokyo);
        //end::s12[]
    }

    static void s13() {
        StreamStage<Entry<String, Long>> latencies = null;
        //tag::s13[]
        StreamStage<Entry<String, Long>> topLatencies = latencies
            .groupingKey(Entry::getKey)
            .mapStateful(
                MINUTES.toMillis(2),
                LongAccumulator::new,
                (topLatencyState, key, e) -> {
                    long currLatency = e.getValue();
                    long topLatency = topLatencyState.get();
                    topLatencyState.set(Math.max(currLatency, topLatency));
                    return currLatency > topLatency
                        ? entry(String.format("%s:newRecord", key), e.getValue())
                        : null;
                },
                (topLatencyState, key, time) -> entry(String.format(
                    "%s:maxForSession:%d", key, time), topLatencyState.get())
            );
        //end::s13[]
    }

    private static StreamStage<Trade> tradeStream(String name) {
        throw new UnsupportedOperationException();
    }

    static void apply() {
        Pipeline p = Pipeline.create();
        BatchSource<String> source = null;
        //tag::apply1[]
        p.readFrom(source)
         .map(String::toLowerCase)
         .filter(s -> s.startsWith("success"))
         .aggregate(counting())
        //end::apply1[]
        ;

        //tag::apply3[]
        p.readFrom(source)
         .apply(PipelineTransforms::cleanUp)
         .aggregate(counting())
        //end::apply3[]
        ;
    }

    static class PipelineTransforms {
        //tag::apply2[]
        static BatchStage<String> cleanUp(BatchStage<String> input) {
            return input.map(String::toLowerCase)
                        .filter(s -> s.startsWith("success"));
        }
        //end::apply2[]
    }

    private static Traverser<String> fooFlatMap(String t) {
        return null;
    }

    private static String fooMap(String t) {
        return null;
    }

    //tag::custom-transform-1[]
    public static class IdentityMapP extends AbstractProcessor {
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            return tryEmit(item);
        }
    }
    //end::custom-transform-1[]

    static void customTransform2() {
        Pipeline p = Pipeline.create();
        BatchSource<String> source = null;
        //tag::custom-transform-2[]
        p.readFrom(source)
         .customTransform("name", IdentityMapP::new)
        //end::custom-transform-2[]
        ;
    }

    static class Tweet {
        String[] hashtags() {
            return null;
        }
    }
}
