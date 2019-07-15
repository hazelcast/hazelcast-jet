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
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;

/**
 * An immutable snapshot of job-specific metrics.
 */
@Beta
public final class JobMetrics implements Serializable {

    private static final JobMetrics EMPTY = new JobMetrics(Collections.emptyMap());

    private final Map<String, Long> metrics;

    private JobMetrics(@Nonnull Map<String, Long> metrics) {
        this.metrics = Collections.unmodifiableMap(metrics);
    }

    /** Builds a {@link JobMetrics} object based on a key-value map of metrics data. */
    public static JobMetrics of(@Nonnull Map<String, Long> metrics) {
        if (metrics.isEmpty()) {
            return EMPTY;
        }
        return new JobMetrics(new HashMap<>(metrics));
    }

    /**
     * Returns an unmodifiable map view of these metrics.
     */
    @Nonnull
    public Map<String, Long> asMap() {
        return metrics;
    }

    /**
     * Returns a collection containing the names of all job-specific metrics
     * available.
     */
    @Nonnull
    public Collection<String> getMetricNames() {
        return metrics.keySet();
    }

    /**
     * Returns the value of a job-specific metric with the given name. Returns
     * null if the metric doesn't exist.
     */
    @Nullable
    public Long getMetricValue(@Nonnull String name) {
        return metrics.get(name);
    }

    /**
     * Returns a subset of this metrics which have the specified value for the
     * specified tag.
     */
    public JobMetrics withTag(String tag, String value) {
        Entry<String, String> tagValue = entry(tag, value);
        Map<String, Long> filteredMetrics = metrics.entrySet().stream()
                .filter(entry -> MetricsUtil.parseMetricName(entry.getKey()).contains(tagValue))
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
