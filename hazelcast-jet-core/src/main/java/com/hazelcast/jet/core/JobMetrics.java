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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;

/**
 * An immutable collection of job-specific metrics, where each metric has a name and a
 * {@link Long} numberic value. The name is a {@link String} formed from a comma separated
 * list of tag=value pairs, which may or may not be enclosed by square brackets. Some
 * examples of valid metric names:
 * <ul>
 * <li>[module=jet,job=jobId,exec=execId,vertex=filter,proc=3,unit=count,metric=queuesCapacity]</li>
 * <li>module=jet,job=jobId,exec=execId,vertex=filter,proc=3,unit=count,metric=queuesCapacity</li>
 * </ul>
 * <p>
 * Since all names are built from tag-value pairs it's possible to filter metrics based on tags,
 * that's what the {@link #withTag(String, String)} method does. For a list of possible tag names
 * see {@link MetricTags}.
 *
 * @since 3.2
 */
public final class JobMetrics implements IdentifiedDataSerializable {

    private static final JobMetrics EMPTY = new JobMetrics(Collections.emptyMap());

    private Map<String, Long> metrics;

    JobMetrics() {
        metrics = null;
    }

    private JobMetrics(@Nonnull Map<String, Long> metrics) {
        Objects.requireNonNull(metrics, "metrics");
        this.metrics = Collections.unmodifiableMap(metrics);
    }

    /**
     * Builds a {@link JobMetrics} object based on a key-value map of metrics data.
     * The key Strings in the map should be well formed metric names (see class javadoc
     * for details).
     */
    @Nonnull
    public static JobMetrics of(@Nonnull Map<String, Long> metrics) {
        Objects.requireNonNull(metrics, "metrics");
        if (metrics.isEmpty()) {
            return EMPTY;
        }
        return new JobMetrics(new HashMap<>(metrics));
    }

    /**
     * Returns an unmodifiable map view of these metrics.
     */
    @Nonnull
    public Map<String, Long> toMap() {
        return metrics;
    }

    /**
     * Returns the name of all metrics contained in this {@link JobMetrics} object.
     */
    @Nonnull
    public Set<String> getMetricNames() {
        return metrics.keySet();
    }

    /**
     * Returns the value of the metric with the given name, if present. Returns
     * null otherwise.
     */
    @Nullable
    public Long getMetricValue(@Nonnull String name) {
        Objects.requireNonNull(name, "name");

        return metrics.get(name);
    }

    /**
     * Returns a new {@link JobMetrics} instance containing a subset of the metrics found
     * in the current instance. The subset is formed by those metrics which have the metric
     * tag with the specified value in their name.
     * <p>
     * For example if we call {@code tag="vertex"} & {@code value="filter"} as the parameters, then the metric
     * named {@code [module=jet,job=jobId,exec=execId,vertex=filter,proc=3,unit=count,metric=queuesCapacity]}
     * will be included while an other one named {@code [module=jet,job=jobId,exec=execId,vertex=map,proc=3,
     * unit=count,metric=queuesCapacity]} will not.
     */
    @Nonnull
    public JobMetrics withTag(@Nonnull String tag, @Nonnull String value) {
        Objects.requireNonNull(tag, "tag");
        Objects.requireNonNull(value, "value");

        Entry<String, String> tagValue = entry(tag, value);
        Map<String, Long> filteredMetrics = metrics.entrySet().stream()
                .filter(entry -> MetricsUtil.parseMetricName(entry.getKey()).contains(tagValue))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new JobMetrics(filteredMetrics);
    }

    /**
     * Returns the number of metrics present.
     */
    public int size() {
        return metrics.size();
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetDataSerializerHook.JOB_METRICS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(metrics);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        metrics = in.readObject();
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
