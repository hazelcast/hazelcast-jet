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
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.core.metrics.Unit;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;

public class MetricsContext {

    private static final BiFunction<String, Unit, Metric> CREATE_SINGLE_WRITER_METRIC = SingleWriterMetric::new;
    private static final BiFunction<String, Unit, Metric> CREATE_THREAD_SAFE_METRICS = ThreadSafeMetric::new;

    private String onlyName;
    private Metric onlyMetric;

    private Map<String, Metric> metrics;

    Metric metric(String name, Unit unit) {
        return metric(name, unit, CREATE_SINGLE_WRITER_METRIC);
    }

    Metric threadSafeMetric(String name, Unit unit) {
        return metric(name, unit, CREATE_THREAD_SAFE_METRICS);
    }

    private Metric metric(String name, Unit unit, BiFunction<String, Unit, Metric> metricSupplier) {
        if (metrics == null) { //at most one already defined metric
            if (onlyMetric == null) { //no already defined metrics
                onlyMetric = metricSupplier.apply(name, unit);
                onlyName = name;
                return onlyMetric;
            } else { //one single already defined metric
                if (name.equals(onlyName)) { //single already defined metric same as the requested one
                    return onlyMetric;
                } else { //single already defined metric different from the requested one
                    metrics = new HashMap<>();
                    metrics.put(onlyName, onlyMetric);

                    onlyMetric = null;
                    onlyName = null;

                    Metric metric = metricSupplier.apply(name, unit);
                    metrics.put(name, metric);
                    return metric;
                }
            }
        } else { //multiple metrics already defined
            Metric metric = metrics.get(name);
            if (metric == null) { //requested metric not yet defined
                metric = metricSupplier.apply(name, unit);
                metrics.put(name, metric);
            }
            return metric;
        }
    }

    public void collectMetrics(MetricTagger tagger, MetricsCollectionContext context) {
        if (onlyMetric != null) {
            MetricTagger withUserFlag = addUserTag(tagger);
            context.collect(withUserFlag, onlyName, ProbeLevel.INFO, toProbeUnit(onlyMetric.unit()), onlyMetric.get());
        } else if (metrics != null) {
            MetricTagger withUserFlag = addUserTag(tagger);
            metrics.forEach((name, metric) ->
                    context.collect(withUserFlag, name, ProbeLevel.INFO, toProbeUnit(metric.unit()), metric.get()));
        }
    }

    private MetricTagger addUserTag(MetricTagger tagger) {
        return tagger.withTag(MetricTags.USER, "true");
    }

    private ProbeUnit toProbeUnit(Unit unit) {
        switch (unit) {
            case BYTES:
                return ProbeUnit.BYTES;
            case MS:
                return ProbeUnit.MS;
            case PERCENT:
                return ProbeUnit.PERCENT;
            case COUNT:
                return ProbeUnit.COUNT;
            case BOOLEAN:
                return ProbeUnit.BOOLEAN;
            case ENUM:
                return ProbeUnit.ENUM;
            default:
                throw new RuntimeException("Unhandled metrics unit " + unit);
        }
    }

    private static final class SingleWriterMetric implements Metric {

        private static final AtomicLongFieldUpdater<SingleWriterMetric> VOLATILE_VALUE_UPDATER =
                AtomicLongFieldUpdater.newUpdater(SingleWriterMetric.class, "value");

        private final String name;
        private final Unit unit;
        private volatile long value;

        SingleWriterMetric(String name, Unit unit) {
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
        public void increment() {
            VOLATILE_VALUE_UPDATER.lazySet(this, value + 1);
        }

        @Override
        public void increment(long increment) {
            VOLATILE_VALUE_UPDATER.lazySet(this, value + increment);
        }

        @Override
        public void decrement() {
            VOLATILE_VALUE_UPDATER.lazySet(this, value - 1);
        }

        @Override
        public void decrement(long decrement) {
            VOLATILE_VALUE_UPDATER.lazySet(this, value - decrement);
        }

        @Override
        public long get() {
            return value;
        }
    }

    private static final class ThreadSafeMetric implements Metric {

        private static final AtomicLongFieldUpdater<ThreadSafeMetric> VOLATILE_VALUE_UPDATER =
                AtomicLongFieldUpdater.newUpdater(ThreadSafeMetric.class, "value");

        private final String name;
        private final Unit unit;
        private volatile long value;

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
        public void increment() {
            VOLATILE_VALUE_UPDATER.incrementAndGet(this);
        }

        @Override
        public void increment(long amount) {
            VOLATILE_VALUE_UPDATER.addAndGet(this, amount);
        }

        @Override
        public void decrement() {
            VOLATILE_VALUE_UPDATER.decrementAndGet(this);
        }

        @Override
        public void decrement(long amount) {
            VOLATILE_VALUE_UPDATER.addAndGet(this, -amount);
        }

        @Override
        public void set(long newValue) {
            VOLATILE_VALUE_UPDATER.set(this, newValue);
        }

        @Override
        public long get() {
            return VOLATILE_VALUE_UPDATER.get(this);
        }
    }
}
