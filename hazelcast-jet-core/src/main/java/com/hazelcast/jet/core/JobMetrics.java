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

package com.hazelcast.jet.core;

import com.hazelcast.internal.metrics.MetricsUtil;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Contains information related to job-specific metrics.
 */
@Beta
public final class JobMetrics implements Serializable {

    /** Static empty instance, contains no metrics */
    public static final JobMetrics EMPTY = new JobMetrics();

    private final Map<String, Long> metrics;

    /** Builds an empty {@link JobMetrics} object. */
    private JobMetrics() {
        this(Collections.emptyMap());
    }

    /** Builds a {@link JobMetrics} object based on a key-value map of metrics data. */
    private JobMetrics(Map<String, Long> metrics) {
        Objects.requireNonNull(metrics);
        this.metrics = Collections.unmodifiableMap(metrics);
    }

    /** Builds a {@link JobMetrics} object based on a key-value map of metrics data. */
    public static JobMetrics of(Map<String, Long> metrics) {
        return new JobMetrics(new HashMap<>(metrics));
    }

    /** Returns a collection containing the names of all job specific metrics available. */
    @Nonnull
    Collection<String> getMetricNames() {
        return metrics.keySet();
    }

    /**
     * Retruns the value of a job specific metric with the given name.
     * @throws IllegalArgumentException if name is null or if no job metric with this name is available.
     */
    Long getMetricValue(String name) {
        Objects.requireNonNull(name);
        return metrics.get(name);
    }

    /** Merges two immutable {@link JobMetrics} instances into a third */
    public JobMetrics merge(JobMetrics other) {
        Map<String, Long> map = new HashMap<>();
        map.putAll(this.metrics);
        map.putAll(other.metrics);
        return new JobMetrics(map);
    }

    /** Returns a subset of the current metrics, which have the specified value for the specified tag. */
    public JobMetrics withTag(String tag, String value) {
        String nameToken = tag + '=' + MetricsUtil.escapeMetricNamePart(value);
        Predicate<Map.Entry<String, Long>> filterPredicate = e ->
                new HashSet<>(Arrays.asList(e.getKey().split("[,\\]\\[]"))).contains(nameToken);
        Map<String, Long> filteredMetrics = metrics.entrySet().stream()
                .filter(filterPredicate)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new JobMetrics(filteredMetrics);
    }

    /** Returns the number of metrics present.*/
    public int size() {
        return metrics.size();
    }

    @Override
    public int hashCode() {
        return metrics.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        return Objects.equals(metrics, ((JobMetrics) obj).metrics);
    }

    @Override
    public String toString() {
        return JobMetrics.class.getSimpleName() + "{" + metrics + "}";
    }
}
