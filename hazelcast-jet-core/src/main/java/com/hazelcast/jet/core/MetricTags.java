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

import com.hazelcast.internal.metrics.Probe;

/**
 * Metric names are formed from a comma separated list of {@code tag_name=tag_value}
 * pairs. The constants defined here are the possible tag names that are being used
 * in Jet. See individual descriptions for the meaning of information carried by
 * each tag.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class MetricTags {

    /**
     * Source system or module, typical value is {@code "jet"},
     */
    public static final String MODULE = "module";

    /**
     * Unique ID of the job (sourcing the metric), example value would be a numerical (long)
     * ID encoded in a human readable form, like {@code "2f7f-d88a-4669-6195"}, see
     * {@link com.hazelcast.jet.Util#idToString(long)})} for details.
     */
    public static final String JOB = "job";

    /**
     * Unique ID of a particular execution of a job (sourcing the metric), example value
     * would be a numerical (long) ID encoded in a human readable form, like
     * {@code "2f7f-d88a-4669-6195"}, see {@link com.hazelcast.jet.Util#idToString(long)}
     * for details.
     */
    public static final String EXECUTION = "exec";

    /**
     * DAG vertex providing a specific metric. Example value would be
     * {@code "fused(map, filter)"}.
     */
    public static final String VERTEX = "vertex";

    /**
     * Global index of the {@link Processor} sourcing the metric.
     */
    public static final String PROCESSOR = "proc";

    /**
     * Simple class name of the {@link Processor} sourcing the metric.
     */
    public static final String PROCESSOR_TYPE = "procType";

    /**
     * Boolean flag which is true if the {@link Processor} sourcing the metric is a
     * DAG source.
     */
    public static final String SOURCE = "source";

    /**
     * Boolean flag which is true if the {@link Processor} sourcing the metric is a
     * DAG sink.
     */
    public static final String SINK = "sink";

    /**
     * Index of cooperative worker in a fixed worker pool sourcing the metric.
     */
    public static final String COOPERATIVE_WORKER = "cooperativeWorker";

    /**
     * Index of vertex input or output edges sourcing the metric.
     */
    public static final String ORDINAL = "ordinal";

    /**
     * Short name of metric in its source class. For details see {@link Probe#name()}.
     */
    public static final String METRIC = "metric";

    /**
     * Unit of metric value, for details see {@link com.hazelcast.internal.metrics.ProbeUnit}.
     */
    public static final String UNIT = "unit";

    private MetricTags() {
    }
}
