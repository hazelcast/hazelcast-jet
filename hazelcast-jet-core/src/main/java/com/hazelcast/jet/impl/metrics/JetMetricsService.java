/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.metrics.ConcurrentArrayRingbuffer.RingbufferSlice;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.Util.entry;

public class JetMetricsService implements ManagedService {

    public static final String SERVICE_NAME = "hz:impl:jetMetricsService";

    public static final int COLLECTION_INTERVAL_SECONDS = 1;
    public static final int METRICS_JOURNAL_CAPACITY = 120;

    private static final int INITIAL_BUFFER_SIZE = 2 << 16;
    private static final int SIZE_FACTOR_NUMERATOR = 11;
    private static final int SIZE_FACTOR_DENOMINATOR = 10;

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    /**
     * Ringbuffer which stores a bounded history of metrics. For each round of collection,
     * the metrics are compressed into a blob and stored along with the timestamp,
     * with the format (timestamp, byte[])
     */
    // TODO [viliam] make the length configurable
    private final ConcurrentArrayRingbuffer<Map.Entry<Long, byte[]>> metricsJournal =
            new ConcurrentArrayRingbuffer<>(METRICS_JOURNAL_CAPACITY);


    public JetMetricsService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        // if capacity is 0, metric collection is disabled
        if (metricsJournal.getCapacity() > 0) {
            int[] lastSize = {INITIAL_BUFFER_SIZE};
            nodeEngine.getExecutionService().scheduleWithRepetition("MetricsForMcCollection", () -> {
                CompressingProbeRenderer renderer = new CompressingProbeRenderer(
                        lastSize[0] * SIZE_FACTOR_NUMERATOR / SIZE_FACTOR_DENOMINATOR);
                this.nodeEngine.getMetricsRegistry().render(renderer);
                byte[] blob = Util.uncheckCall(() -> IOUtil.compress(renderer.getRenderedBlob()));
                lastSize[0] = blob.length;
                metricsJournal.add(entry(System.currentTimeMillis(), blob));
                LoggingUtil.logFine(logger, "Collected %,d metrics, %,d bytes", renderer.getCount(), blob.length);
            }, COLLECTION_INTERVAL_SECONDS, COLLECTION_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    public RingbufferSlice<Map.Entry<Long, byte[]>> getMetricBlobs(long startSequence) {
        return metricsJournal.copyFrom(startSequence);
    }


    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }
}
