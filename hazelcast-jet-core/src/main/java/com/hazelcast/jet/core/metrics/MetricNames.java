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

import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Vertex;

/**
 * Each metric provided by Jet has a specific name which conceptually
 * identifies what it's being used to measure. Besides their name metrics
 * also have a description made up by tags, but those are more like
 * attributes which describe a specific instance of the metric and are not
 * directly part of the identity of the metric.
 * <p>
 * The constants described here represent the various names metrics can
 * take in Jet.
 */
public final class MetricNames {

    /**
     * Counts incoming data items on input {@link Edge}s of particular
     * {@link Vertex} instances running on various {@link Processor}s.
     * These in turn can be identified based on the
     * {@link MetricTags#ORDINAL}, {@link MetricTags#VERTEX} &
     * {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String RECEIVED_COUNT = "receivedCount";

    /**
     * Counts incoming data item batches on input {@link Edge}s of particular
     * {@link Vertex} instances running on various {@link Processor}s.
     * These in turn can be identified based on the
     * {@link MetricTags#ORDINAL}, {@link MetricTags#VERTEX} &
     * {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String RECEIVED_BATCHES = "receivedBatches";

    /**
     * Tracks the total size of all input queues of a particular
     * {@link Vertex} on a particular {@link Processor}. They both can be
     * identified based on the {@link MetricTags#VERTEX} and
     * {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String QUEUES_SIZE = "queuesSize";

    /**
     * Tracks the total capacity of all input queues of a particular
     * {@link Vertex} on a particular {@link Processor}. They both can be
     * identified based on the {@link MetricTags#VERTEX} and
     * {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String QUEUES_CAPACITY = "queuesCapacity";

    /**
     * Counts the data items emitted on outgoing {@link Edge}s of particular
     * {@link Vertex} instances running on various {@link Processor}s.
     * These in turn can be identified based on the
     * {@link MetricTags#ORDINAL}, {@link MetricTags#VERTEX} &
     * {@link MetricTags#PROCESSOR} tags of the metric.
     * <p>
     * When the {@link MetricTags#ORDINAL} tag contains the value "snapshot"
     * then the count contains the number of snapshots saved by the
     * {@link Processor}.
     */
    public static final String EMITTED_COUNT = "emittedCount";

    /**
     * Tracks the highest coalesced watermark observed on all input
     * {@link Edge}s of a particular {@link Vertex} (ie. the highest
     * watermark observed on all input queues of that {@link Vertex}).
     * The {@link Vertex} and the {@link Processor} can be identified
     * based on the {@link MetricTags#VERTEX} & {@link MetricTags#PROCESSOR}
     * tags of the metric.
     */
    public static final String TOP_OBSERVED_WM = "topObservedWm";

    /**
     * Tracks the highest watermark observed on all the input queues
     * of a particular incoming {@link Edge} of a certain {@link Vertex}.
     * The {@link Vertex} and the {@link Processor} can be identified
     * based on the {@link MetricTags#VERTEX} & {@link MetricTags#PROCESSOR}
     * tags of the metric.
     */
    public static final String COALESCED_WM = "coalescedWm";

    /**
     * Tracks the last watermark emitted by a particular {@link Processor}
     * of a particular {@link Vertex}. The {@link Vertex} and the
     * {@link Processor} can be identified based on the {@link MetricTags#VERTEX}
     * & {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String LAST_FORWARDED_WM = "lastForwardedWm";

    /**
     * Tracks the difference between the last emitted watermark and the
     * system time of a particular {@link Processor} of a particular
     * {@link Vertex}. The {@link Vertex} and the {@link Processor} can be
     * identified based on the {@link MetricTags#VERTEX} &
     * {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String LAST_FORWARDED_WM_LATENCY = "lastForwardedWmLatency";

    /**
     * Tracks the total number of bytes written out in the last snapshot
     * by a particular {@link Vertex}. The name of the vertex can be found
     * in the {@link MetricTags#VERTEX} tag of the metric.
     */
    public static final String SNAPSHOT_BYTES = "snapshotBytes";

    /**
     * Tracks the total number of keys written out in the last snapshot
     * by a particular {@link Vertex}. The name of the vertex can be found
     * in the {@link MetricTags#VERTEX} tag of the metric.
     */
    public static final String SNAPSHOT_KEYS = "snapshotKeys";

    /**
     * Tracks the total number of chunks written out in the last snapshot
     * by a particular {@link Vertex}. The name of the vertex can be found
     * in the {@link MetricTags#VERTEX} tag of the metric.
     */
    public static final String SNAPSHOT_CHUNKS = "snapshotChunks";

    /**
     * Tracks the start time of a given execution of a specific job.
     * The execution and the job can be identified based on the
     * {@link MetricTags#EXECUTION} & {@link MetricTags#JOB} tags of
     * the metric.
     */
    public static final String EXECUTION_START_TIME = "executionStartTime";

    /**
     * Tracks the total amount of time since a given execution of a specific
     * job is running. The execution and the job can be identified based
     * on the {@link MetricTags#EXECUTION} & {@link MetricTags#JOB} tags
     * of the metric.
     */
    public static final String EXECUTION_DURATION = "executionDuration";

    /**
     * Counts data items coming in over the network for DISTRIBUTED input
     * {@link Edge}s of particular {@link Vertex} instances running on
     * various {@link Processor}s. These in turn can be identified based
     * on the {@link MetricTags#ORDINAL}, {@link MetricTags#VERTEX} &
     * {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String DISTRIBUTED_ITEMS_IN = "distributedItemsIn";

    /**
     * Tracks the data volume (bytes) coming in over the network for
     * DISTRIBUTED input {@link Edge}s of particular {@link Vertex}
     * instances running on various {@link Processor}s. These in turn can
     * be identified based on the {@link MetricTags#ORDINAL},
     * {@link MetricTags#VERTEX} & {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String DISTRIBUTED_BYTES_IN = "distributedBytesIn";

    /**
     * Counts data items going out over the network for DISTRIBUTED output
     * {@link Edge}s of particular {@link Vertex} instances running on
     * various {@link Processor}s. These in turn can be identified based
     * on the {@link MetricTags#ORDINAL}, {@link MetricTags#VERTEX} &
     * {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String DISTRIBUTED_ITEMS_OUT = "distributedItemsOut";

    /**
     * Tracks the data volume (bytes) going out over the network for
     * DISTRIBUTED output {@link Edge}s of particular {@link Vertex}
     * instances running on various {@link Processor}s. These in turn can
     * be identified based on the {@link MetricTags#ORDINAL},
     * {@link MetricTags#VERTEX} & {@link MetricTags#PROCESSOR} tags of the metric.
     */
    public static final String DISTRIBUTED_BYTES_OUT = "distributedBytesOut";

    /**
     * Number of computational jobs submitted to a particular Jet cluster
     * member.
     */
    public static final String JOBS_SUBMITTED = "jobs.submitted";

    /**
     * Number of computational jobs successfully completed by a particular
     * Jet cluster member.
     */
    public static final String JOBS_COMPLETED_SUCCESSFULLY = "jobs.completedSuccessfully";

    /**
     * Number of computational jobs that have failed on a particular Jet
     * cluster member.
     */
    public static final String JOBS_COMPLETED_WITH_FAILURE = "jobs.completedWithFailure";

    /**
     * Number of computational job executions started on a particular Jet
     * cluster member. Each job can execute multiple times, for example when
     * it's restarted or suspended and then resumed.
     */
    public static final String JOB_EXECUTIONS_STARTED = "jobs.executionStarted";

    /**
     * Number of computational job executions finished on a particular Jet
     * cluster member. Each job can execute multiple times, for example when
     * it's restarted or suspended and then resumed.
     */
    public static final String JOB_EXECUTIONS_TERMINATED = "jobs.executionTerminated";

    private MetricNames() {
    }

}
