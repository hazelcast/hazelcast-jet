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
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.Util.memoize;

public class GenerateTradesP extends AbstractProcessor {

    public static final AtomicLong tradeCount = new AtomicLong();

    private final Traverser<Trade> traverser;

    private final Map<String, Integer> tickerToPrice = new HashMap<>();

    private final Supplier<String[]> tickers = memoize(() -> tickerToPrice.keySet().toArray(new String[0]));
    private int tradeCountLocal;

    GenerateTradesP(int periodMillis) {
        Traverser<Trade> traverser = new Traverser<Trade>() {
            int tickerIndex;
            long lag;

            @Override
            public Trade next() {
                String[] tickers = GenerateTradesP.this. tickers.get();
                String ticker = tickers[tickerIndex++];
                if (tickerIndex == tickers.length) {
                    tickerIndex = 0;
                }
                lag++;
                if (lag == 2000)
                    lag = 0;
                tradeCountLocal++;
                if (tradeCountLocal == 1000) {
                    tradeCount.addAndGet(tradeCountLocal);
                    tradeCountLocal = 0;
                }
                return new Trade(System.currentTimeMillis() - lag, ticker, 100, 10000);
            }
        };
        this.traverser = periodMillis > 0 ? new PeriodicTraverser<>(traverser, periodMillis) : traverser;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Map.Entry<String, Integer> initial = (Entry<String, Integer>) item;
        tickerToPrice.put(initial.getKey(), initial.getValue());
        return true;
    }

    @Override
    public boolean complete() {
        if (tickerToPrice.isEmpty()) {
            return true;
        }
        emitFromTraverser(traverser);
        return false;
    }

    static class PeriodicTraverser<T> implements Traverser<T> {

        private final Traverser<T> traverser;
        private final int periodMillis;

        private long last;

        PeriodicTraverser(Traverser<T> traverser, int periodMillis) {
            this.traverser = traverser;
            this.periodMillis = periodMillis;
        }

        @Override
        public T next() {
            long curr = System.currentTimeMillis();
            if (curr - last > periodMillis) {
                last = curr;
                return traverser.next();
            }
            return null;
        }
    }
}
