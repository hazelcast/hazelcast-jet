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

package com.hazelcast.jet.examples.tradesource;

import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class TradeGenerator {
    private static final int LOT = 100;
    private static final long PRICE_UNITS_PER_CENT = 1000L;
    private static final int NO_TIMEOUT = 0;

    private final List<String> tickers;
    private final long emitPeriodNanos;
    private final long startTimeMillis;
    private final long startTimeNanos;
    private final long endTimeNanos;
    private final long maxLagNanos;
    private final Map<String, LongLongAccumulator> pricesAndTrends;

    private long scheduledTimeNanos;

    private TradeGenerator(long numTickers, int tradesPerSec, int maxLagMillis, long timeoutSeconds) {
        this.tickers = loadTickers(numTickers);
        this.maxLagNanos = MILLISECONDS.toNanos(maxLagMillis);
        this.pricesAndTrends = tickers.stream()
                .collect(toMap(t -> t, t -> new LongLongAccumulator(500, 5)));
        this.emitPeriodNanos = SECONDS.toNanos(1) / tradesPerSec;
        this.startTimeNanos = this.scheduledTimeNanos = System.nanoTime();
        this.endTimeNanos = timeoutSeconds <= 0 ? Long.MAX_VALUE :
                startTimeNanos + SECONDS.toNanos(timeoutSeconds);
        this.startTimeMillis = System.currentTimeMillis();
    }

    /**
     * Returns a source of an unbounded stream of trade events. The
     * quantity of the returned trades will be a random multiple of an
     * internal lot size ({@value #LOT}). The price will also be
     * randomized, but not as an absolute value, but as a value with
     * randomly varying change trends up or down. Both the price and the
     * quantity are always positive.
     * <p>
     * The trades will be generated at the specified rate per second and
     * their tickers will be from a limited set of names.
     * <p>
     * The event time of the generated trades will be evenly spaced
     * across physical time, as dictated by the trades-per-second
     * generation rate (ie. no disorder will be introduced by the source).
     *
     * @param tradesPerSec number of trades generated per second
     *                     (globally, for all tickers)
     * @return streaming source of trades
     */
    public static StreamSource<Trade> tradeSource(int tradesPerSec) {
        return tradeSource(Integer.MAX_VALUE, tradesPerSec, 0);
    }

    /**
     * Returns a source of an unbounded stream of trade events. The
     * quantity of the returned trades will be a random multiple of an
     * internal lot size ({@value #LOT}). The price will also be
     * randomized, but not as an absolute value, but as a value with
     * randomly varying change trends up or down. Both the price and the
     * quantity are always positive.
     * <p>
     * The trades will be generated at the specified rate per second and
     * their tickers will be from a limited set of names, no more than
     * the specified limit.
     * <p>
     * The event time of the generated trades will be evenly spaced
     * across physical time, as dictated by the trades-per-second
     * generation rate (ie. no disorder will be introduced by the source).
     *
     * @param numTickers   maximum number of ticker names for which
     *                     trades will be generated
     * @param tradesPerSec number of trades generated per second
     *                     (globally, for all tickers)
     * @return streaming source of trades
     */
    public static StreamSource<Trade> tradeSource(int numTickers, int tradesPerSec) {
        return tradeSource(numTickers, tradesPerSec, 0);
    }

    /**
     * Returns a source of an unbounded stream of trade events. The
     * quantity of the returned trades will be a random multiple of an
     * internal lot size ({@value #LOT}). The price will also be
     * randomized, but not as an absolute value, but as a value with
     * randomly varying change trends up or down. Both the price and the
     * quantity are always positive.
     * <p>
     * The trades will be generated at the specified rate per second and
     * their tickers will be from a limited set of names, no more than
     * the specified limit.
     * <p>
     * The event time of the generated trades would normally be evenly
     * spaced across physical time, as dictated by the trades-per-second
     * generation rate. However the trade source will introduce
     * randomness in these timestamps, but no more than the specified
     * upper limit.
     *
     * @param numTickers   maximum number of ticker names for which
     *                     trades will be generated
     * @param tradesPerSec number of trades generated per second
     *                     (globally, for all tickers)
     * @param maxLag       maximum random variance in the event time of
     *                     the trades
     * @return streaming source of trades
     */
    public static StreamSource<Trade> tradeSource(int numTickers, int tradesPerSec, int maxLag) {
        return SourceBuilder
                .timestampedStream("trade-source",
                        x -> new TradeGenerator(numTickers, tradesPerSec, maxLag, NO_TIMEOUT))
                .fillBufferFn(TradeGenerator::generateTrades)
                .build();
    }

    private static long getNextPrice(LongLongAccumulator priceAndDelta, ThreadLocalRandom rnd) {
        long price = priceAndDelta.get1();
        long delta = priceAndDelta.get2();
        if (price + delta <= 0) {
            //having a negative price doesn't make sense for most financial instruments
            delta = -delta;
        }
        price = price + delta;
        delta = delta + rnd.nextLong(101) - 50;

        priceAndDelta.set1(price);
        priceAndDelta.set2(delta);

        return price;
    }

    private static List<String> loadTickers(long numTickers) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TradeGenerator.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))) {
            return reader.lines()
                    .skip(1)
                    .limit(numTickers)
                    .map(l -> l.split("\\|")[0])
                    .collect(toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void generateTrades(TimestampedSourceBuffer<Trade> buf) {
        if (scheduledTimeNanos >= endTimeNanos) {
            return;
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long nowNanos = System.nanoTime();
        while (scheduledTimeNanos <= nowNanos) {
            String ticker = tickers.get(rnd.nextInt(tickers.size()));
            LongLongAccumulator priceAndDelta = pricesAndTrends.get(ticker);
            long price = getNextPrice(priceAndDelta, rnd) / PRICE_UNITS_PER_CENT;
            long tradeTimeNanos = scheduledTimeNanos - (maxLagNanos > 0 ? rnd.nextLong(maxLagNanos) : 0L);
            long tradeTimeMillis = startTimeMillis + NANOSECONDS.toMillis(tradeTimeNanos - startTimeNanos);
            Trade trade = new Trade(tradeTimeMillis, ticker, rnd.nextInt(1, 10) * LOT, price);
            buf.add(trade, tradeTimeMillis);
            scheduledTimeNanos += emitPeriodNanos;
            if (scheduledTimeNanos > nowNanos) {
                // Refresh current time before checking against scheduled time
                nowNanos = System.nanoTime();
            }
        }
    }
}
