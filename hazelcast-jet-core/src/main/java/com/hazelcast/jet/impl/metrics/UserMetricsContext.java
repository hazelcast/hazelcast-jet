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
import com.hazelcast.jet.core.metrics.Gauge;
import com.hazelcast.jet.core.metrics.Unit;
import com.hazelcast.jet.impl.execution.ProcessorTasklet;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class UserMetricsContext {

    private final ProbeBuilder probeBuilder;
    private final ProcessorTasklet source;
    private final ProbeLevel level;

    private String onlyName;
    private Metric onlyMetric;
    private Map<String, Metric> metrics;

    public UserMetricsContext(ProbeBuilder probeBuilder, ProcessorTasklet source, ProbeLevel level) {
        this.probeBuilder = probeBuilder;
        this.source = source;
        this.level = level;
    }

    Counter getCounter(String name) {
        return getMetric(name, Unit.COUNT);
    }

    Gauge getGauge(String name, Unit unit) {
        return getMetric(name, unit);
    }

    private Metric getMetric(String name, Unit unit) {
        Metric res = null;
        if (name.equals(onlyName)) {
            res = onlyMetric;
        } else if (metrics != null) {
            res = metrics.get(name);
        }
        // register metric on first use
        if (res == null) {
            res = initMetric(name, unit);
            if (onlyName == null) {
                // the first and so far the only metric
                onlyName = name;
                onlyMetric = res;
            } else {
                // 2 or more metrics
                if (metrics == null) {
                    metrics = new HashMap<>();
                    metrics.put(onlyName, onlyMetric);
                    onlyName = null;
                    onlyMetric = null;
                }
                metrics.put(name, res);
            }
        }
        return res;
    }

    private Metric initMetric(String name, Unit unit) {
        if (probeBuilder == null) {
            // used when metrics are disabled
            return new NonRegisteredMetric(name);
        } else {
            RegisteredMetric metric = new RegisteredMetric(name);
            LongProbeFunction<ProcessorTasklet> longProbeFunction = t -> metric.get();
            probeBuilder.register(source, name, level, toProbeUnit(unit), longProbeFunction);
            return metric;
        }
    }

    private ProbeUnit toProbeUnit(Unit unit) {
        switch (unit) {
            case COUNT:
                return ProbeUnit.COUNT;
            case MS:
                return ProbeUnit.MS;
            case BYTES:
                return ProbeUnit.BYTES;
            default:
                throw new RuntimeException("Unhandled metrics unit " + unit);
        }
    }

    private interface Metric extends Counter, Gauge {

        long get();

    }

    private static final class RegisteredMetric implements Metric {

        private static final AtomicLongFieldUpdater<RegisteredMetric> VOLATILE_VALUE_UPDATER =
                AtomicLongFieldUpdater.newUpdater(RegisteredMetric.class, "value");

        private final String name;
        private volatile long value;

        RegisteredMetric(String name) {
            this.name = name;
        }

        @Nonnull @Override
        public String name() {
            return name;
        }

        @Override
        public void set(long newValue) {
            VOLATILE_VALUE_UPDATER.lazySet(this, newValue);
        }

        @Override
        public void inc() {
            VOLATILE_VALUE_UPDATER.lazySet(this, value + 1);
        }

        @Override
        public void add(long increment) {
            VOLATILE_VALUE_UPDATER.lazySet(this, value + increment);
        }

        @Override
        public void dec() {
            VOLATILE_VALUE_UPDATER.lazySet(this, value - 1);
        }

        @Override
        public void sub(long decrement) {
            VOLATILE_VALUE_UPDATER.lazySet(this, value - decrement);
        }

        @Override
        public long get() {
            return value;
        }
    }

    private static final class NonRegisteredMetric implements Metric {

        private final String name;
        private long value;

        NonRegisteredMetric(String name) {
            this.name = name;
        }

        @Nonnull @Override
        public String name() {
            return name;
        }

        @Override
        public void set(long newValue) {
            this.value = newValue;
        }

        @Override
        public void inc() {
            this.value++;
        }

        @Override
        public void add(long increment) {
            this.value += increment;
        }

        @Override
        public void dec() {
            this.value--;
        }

        @Override
        public void sub(long decrement) {
            this.value -= decrement;
        }

        @Override
        public long get() {
            return value;
        }
    }
}
