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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.jet.impl.JobMetricsUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;

/**
 * An immutable collection of job-specific metrics, pairs of metric names and sets of associated {@link Measurement}s.
 *
 * @since 3.2
 */
public final class JobMetrics implements IdentifiedDataSerializable {

    private static final JobMetrics EMPTY = new JobMetrics(Collections.emptyMap());

    private static final Collector<Measurement, ?, TreeMap<String, Set<Measurement>>> COLLECTOR = Collectors.groupingBy(
            measurement -> measurement.getTag(MetricTags.METRIC),
            TreeMap::new,
            Collectors.mapping(identity(), toSet())
    );

    private Map<String, Set<Measurement>> metrics; //metric name -> set of measurements

    JobMetrics() { //needed for deserialization
    }

    private JobMetrics(@Nonnull Map<String, Set<Measurement>> metrics) {
        this.metrics = new HashMap<>(metrics);
    }

    /**
     * Returns an empty {@link JobMetrics} object.
     */
    @Nonnull
    public static JobMetrics empty() {
        return EMPTY;
    }

    /**
     * Builds a {@link JobMetrics} object based on one global timestamp and a key-value map of raw metrics data. The key
     * {@code String}s in the map should be well formed metric descriptors and the values associated with them are {@code
     * long} numbers.
     * <p>
     * Descriptors are {@code String}s structured as a comma separated lists of tag=value pairs, enclosed in square
     * brackets. An example of a valid metric descriptor would be:
     * <pre>{@code
     *      [module=jet,job=jobId,exec=execId,vertex=filter,proc=3,unit=count,metric=queuesCapacity]
     * }</pre>
     */
    @Nonnull
    public static JobMetrics of(long timestamp, @Nonnull Map<String, Long> metrics) {
        Objects.requireNonNull(metrics, "metrics");
        if (metrics.isEmpty()) {
            return EMPTY;
        }
        return new JobMetrics(parseRawMetrics(timestamp, metrics));
    }

    private static Map<String, Set<Measurement>> parseRawMetrics(long timestamp, Map<String, Long> raw) {
        HashMap<String, Set<Measurement>> parsed = new HashMap<>();
        for (Entry<String, Long> rawEntry : raw.entrySet()) {
            Long value = rawEntry.getValue();
            if (value == null) {
                throw new IllegalArgumentException("Value missing");
            }

            String descriptor = rawEntry.getKey();
            Map<String, String> tags = JobMetricsUtil.parseMetricDescriptor(descriptor);

            String metricName = tags.get(MetricTags.METRIC);
            if (metricName == null || metricName.isEmpty()) {
                throw new IllegalArgumentException("Metric name missing");
            }

            Set<Measurement> measurements = parsed.computeIfAbsent(metricName, mn -> new HashSet<>());
            measurements.add(Measurement.of(value, timestamp, tags));
        }
        return parsed;
    }

    /**
     * Returns all metrics present.
     */
    @Nonnull
    public Set<String> metrics() {
        return Collections.unmodifiableSet(metrics.keySet());
    }

    /**
     * Returns all {@link Measurement}s associated with a given metric name.
     */
    @Nonnull
    public Set<Measurement> get(@Nonnull String metricName) {
        Objects.requireNonNull(metricName);
        Set<Measurement> measurements = metrics.get(metricName);
        return measurements == null ? Collections.emptySet() : measurements;
    }

    /**
     * Convenience method for {@link #filter(Predicate<Measurement>)}, returns a new {@link JobMetrics} instance containing
     * only those {@link Measurement}s which have the specified tag set to the specified value.
     */
    @Nonnull
    public JobMetrics filter(@Nonnull String tagName, @Nonnull String tagValue) {
        return filter(MeasurementFilters.tagValueEquals(tagName, tagValue));
    }

    /**
     * Returns a new {@link JobMetrics} instance containing a subset of the {@link Measurement}s found in the current one.
     * The subset is formed by those {@link Measurement}s which match the provided {@link Predicate}.
     * <p>
     * The metric names which have all their {@link Measurement}s filtered out won't be present in the new {@link
     * JobMetrics} instance.
     */
    @Nonnull
    public JobMetrics filter(@Nonnull Predicate<Measurement> predicate) {
        Objects.requireNonNull(predicate, "predicate");

        Map<String, Set<Measurement>> filteredMetrics = metrics.values().stream()
                .flatMap(Collection::stream)
                .filter(predicate)
                .collect(COLLECTOR);
        return new JobMetrics(filteredMetrics);
    }

    /**
     * Returns the total number of {@link Measurement}s present (<strong>NOT</strong> just the number of the metric names
     * present).
     */
    public int size() {
        return metrics.values().stream().mapToInt(Set::size).sum();
    }

    /**
     * Returns true if the current instance of {@link JobMetrics} doesn't contain any metric names, thus any {@link
     * Measurement}s either.
     */
    public boolean isEmpty() {
        return metrics.isEmpty();
    }

    /**
     * Merges the current instance of {@link JobMetrics} with the provided one and returns the result as a new {@link
     * JobMetrics} object. The returned object will contain all metric names from both sources and a union of all their
     * {@link Measurement}s.
     */
    @Nonnull
    public JobMetrics merge(@Nonnull JobMetrics that) {
        Objects.requireNonNull(that, "that");
        Stream<Measurement> thisMeasurements = this.metrics.values().stream().flatMap(Collection::stream);
        Stream<Measurement> thatMeasurements = that.metrics.values().stream().flatMap(Collection::stream);
        return new JobMetrics(Stream.concat(thisMeasurements, thatMeasurements).collect(COLLECTOR));
    }

    @Override
    public int getFactoryId() {
        return MetricsDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return MetricsDataSerializerHook.JOB_METRICS;
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
        return metrics.toString();
    }
}
