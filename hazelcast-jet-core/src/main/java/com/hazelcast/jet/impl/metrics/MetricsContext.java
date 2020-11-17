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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.core.metrics.Unit;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class MetricsContext implements DynamicMetricsProvider {

    private static final BiFunction<String, Unit, AbstractMetric> CREATE_SINGLE_WRITER_METRIC = SingleWriterMetric::new;
    private static final BiFunction<String, Unit, AbstractMetric> CREATE_THREAD_SAFE_METRICS = ThreadSafeMetric::new;

    private final MetricsStore store = new MetricsStore();

    Metric metric(String name, Unit unit) {
        return metric(name, unit, CREATE_SINGLE_WRITER_METRIC);
    }

    Metric threadSafeMetric(String name, Unit unit) {
        return metric(name, unit, CREATE_THREAD_SAFE_METRICS);
    }

    private Metric metric(String name, Unit unit, BiFunction<String, Unit, AbstractMetric> metricSupplier) {
        return store.localGet(name, unit, metricSupplier);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor tagger, MetricsCollectionContext context) {
        MetricDescriptor withUserTag = tagger.copy().withTag(MetricTags.USER, "true");

        store.threadSafeForEach((name, metric) ->
                context.collect(withUserTag, name, ProbeLevel.INFO, toProbeUnit(metric.unit()), metric.get()));
    }

    private static MetricDescriptor addUserTag(MetricDescriptor tagger) {
        return tagger.copy().withTag(MetricTags.USER, "true");
    }

    private ProbeUnit toProbeUnit(Unit unit) {
        return ProbeUnit.valueOf(unit.name());
    }

    private abstract static class AbstractMetric implements Metric {

        private final String name;
        private final Unit unit;

        AbstractMetric(String name, Unit unit) {
            this.name = name;
            this.unit = unit;
        }

        @Nonnull @Override
        public String name() {
            return name;
        }

        @Nonnull @Override
        public Unit unit() {
            return unit;
        }

        protected abstract long get();

    }

    private static final class SingleWriterMetric extends AbstractMetric {

        private static final AtomicLongFieldUpdater<SingleWriterMetric> VOLATILE_VALUE_UPDATER =
                AtomicLongFieldUpdater.newUpdater(SingleWriterMetric.class, "value");

        private volatile long value;

        SingleWriterMetric(String name, Unit unit) {
            super(name, unit);
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
        protected long get() {
            return value;
        }
    }

    private static final class ThreadSafeMetric extends AbstractMetric {

        private static final AtomicLongFieldUpdater<ThreadSafeMetric> VOLATILE_VALUE_UPDATER =
                AtomicLongFieldUpdater.newUpdater(ThreadSafeMetric.class, "value");

        private volatile long value;

        ThreadSafeMetric(String name, Unit unit) {
            super(name, unit);
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
        protected long get() {
            return VOLATILE_VALUE_UPDATER.get(this);
        }
    }

    /**
     * Map for storing metrics on a per name basis. Meant to offer fast,
     * single-threaded access, but also a loosely consistent, thread safe
     * way to iterate over the content.
     */
    private static class MetricsStore {

        private Map<String, AbstractMetric> threadLocalStore;
        private volatile ConcurrentMap<String, AbstractMetric> threadSafeStore;

        /**
         * Always called on the same thread, the one executing the Tasklet this context belongs to.
         */
        Metric localGet(String name, Unit unit, BiFunction<String, Unit, AbstractMetric> metricSupplier) {
            if (threadLocalStore == null) { //first metric being stored
                threadLocalStore = new HashMap<>();
                threadSafeStore = new ConcurrentHashMap<>();
            }

            AbstractMetric metric = threadLocalStore.get(name);
            if (metric != null) {
                return metric;
            }

            metric = metricSupplier.apply(name, unit);
            threadLocalStore.put(name, metric);
            threadSafeStore.put(name, metric);

            return metric;
        }

        void threadSafeForEach(BiConsumer<? super String, ? super AbstractMetric> consumer) {
            if (threadSafeStore != null) {
                threadLocalStore.forEach(consumer);
            }
        }
    }

}
