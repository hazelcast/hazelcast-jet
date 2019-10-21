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

import com.hazelcast.internal.metrics.MetricTagger;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Unit;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class MetricsContext {

    private String onlyName;
    private Metric onlyMetric;
    private Unit onlyUnit;

    private Map<String, Metric> metrics;

    Metric metric(String name, Unit unit, boolean threadSafe) {
        Metric res = null;
        if (name.equals(onlyName)) {
            res = onlyMetric;
        } else if (metrics != null) {
            res = metrics.get(name);
        }
        // register metric on first use
        if (res == null) {
            res = threadSafe ? new ThreadSafeMetric(name, unit) : new SimpleMetric(name, unit);
            if (onlyName == null) {
                // the first and so far the only metric
                onlyName = name;
                onlyMetric = res;
                onlyUnit = unit;
            } else {
                // 2 or more metrics
                if (metrics == null) {
                    metrics = new HashMap<>();
                    metrics.put(onlyName, onlyMetric);

                    onlyName = null;
                    onlyMetric = null;
                    onlyUnit = null;
                }
                metrics.put(name, res);
            }
        }
        return res;
    }

    public void collectMetrics(MetricTagger tagger, MetricsCollectionContext context) {
        if (onlyMetric != null) {
            context.collect(tagger, onlyName, ProbeLevel.INFO, toProbeUnit(onlyUnit), onlyMetric.get());
        } else if (metrics != null) {
            metrics.forEach((name, metric) ->
                    context.collect(tagger, name, ProbeLevel.INFO, toProbeUnit(metric.unit()), metric.get()));
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

    private static final class SimpleMetric implements Metric {

        private static final AtomicLongFieldUpdater<SimpleMetric> VOLATILE_VALUE_UPDATER =
                AtomicLongFieldUpdater.newUpdater(SimpleMetric.class, "value");

        private final String name;
        private final Unit unit;
        private volatile long value;

        SimpleMetric(String name, Unit unit) {
            this.name = name;
            this.unit = unit;
        }

        @Nonnull @Override
        public String name() {
            return name;
        }

        @Override
        public Unit unit() {
            return unit;
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

    private static final class ThreadSafeMetric implements Metric {

        private final String name;
        private final Unit unit;
        private AtomicLong value = new AtomicLong();

        ThreadSafeMetric(String name, Unit unit) {
            this.name = name;
            this.unit = unit;
        }

        @Nonnull
        @Override
        public String name() {
            return name;
        }

        @Override
        public Unit unit() {
            return unit;
        }

        @Override
        public void inc() {
            value.incrementAndGet();
        }

        @Override
        public void add(long amount) {
            value.addAndGet(amount);
        }

        @Override
        public void dec() {
            value.decrementAndGet();
        }

        @Override
        public void sub(long amount) {
            value.addAndGet(-amount);
        }

        @Override
        public void set(long newValue) {
            value.set(newValue);
        }

        @Override
        public long get() {
            return value.get();
        }
    }
}
