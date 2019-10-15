/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.ProbeBuilder;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.jet.core.metrics.Counter;
import com.hazelcast.jet.impl.execution.ProcessorTasklet;
import com.hazelcast.jet.impl.util.Util;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class UserMetricsContext {

    private final Function<String, Counter> counterSupplier;

    private String onlyMetric;
    private Counter onlyCounter;
    private Map<String, Counter> metrics;

    public UserMetricsContext(ProbeBuilder probeBuilder, ProcessorTasklet source, ProbeLevel level, ProbeUnit unit) {
        this.counterSupplier = metric -> {
            if (probeBuilder == null) {
                return new UnregisteredCounter(metric);
            } else {
                AtomicLong value = new AtomicLong();
                probeBuilder.register(source, metric, level, unit, (LongProbeFunction<ProcessorTasklet>) t -> value.get());
                return new RegisteredCounter(metric, value);
            }
        };
    }

    Counter get(String metric) {
        Counter res = null;
        if (metric.equals(onlyMetric)) {
            res = onlyCounter;
        } else if (metrics != null) {
            res = metrics.get(metric);
        }
        // register metric on first use
        if (res == null) {
            res = counterSupplier.apply(metric);
            if (onlyMetric == null) {
                // the first on so far the only metric
                onlyMetric = metric;
                onlyCounter = res;
            } else {
                // 2 or more metrics
                if (metrics == null) {
                    metrics = new HashMap<>();
                    metrics.put(onlyMetric, onlyCounter);
                    onlyMetric = null;
                    onlyCounter = null;
                }
                metrics.put(metric, res);
            }
        }
        return res;
    }

    private static final class RegisteredCounter implements Counter {

        private final String name;
        private final AtomicLong value;

        RegisteredCounter(String name, AtomicLong value) {
            this.name = name;
            this.value = value;
        }

        @Nonnull
        @Override
        public String name() {
            return name;
        }

        @Override
        public void set(long newValue) {
            value.lazySet(newValue);
        }

        @Override
        public void increment() {
            Util.lazyAdd(value, 1L);
        }

        @Override
        public void increment(long increment) {
            Util.lazyAdd(value, increment);
        }

        @Override
        public void decrement() {
            Util.lazyAdd(value, -1L);
        }

        @Override
        public void decrement(long decrement) {
            Util.lazyAdd(value, -1L * decrement);
        }

        @Override
        public long value() {
            return value.get();
        }
    }

    private static final class UnregisteredCounter implements Counter {

        private final String name;
        private long value;

        UnregisteredCounter(String name) {
            this.name = name;
        }

        @Nonnull
        @Override
        public String name() {
            return name;
        }

        @Override
        public void set(long newValue) {
            this.value = newValue;
        }

        @Override
        public void increment() {
            this.value++;
        }

        @Override
        public void increment(long increment) {
            this.value += increment;
        }

        @Override
        public void decrement() {
            this.value--;
        }

        @Override
        public void decrement(long decrement) {
            this.value -= decrement;
        }

        @Override
        public long value() {
            return value;
        }
    }

}
