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

import com.hazelcast.config.Config;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.jet.config.MetricsConfig;
import com.hazelcast.jet.impl.metrics.jmx.JmxRenderer;
import com.hazelcast.jet.impl.metrics.jmx.Metrics;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

/**
 * A service to render metrics at regular intervals and store them in a
 * ringbuffer from which the clients can read.
 */
public class JetMetricsService implements ManagedService, ConfigurableService<MetricsConfig> {

    public static final String SERVICE_NAME = "hz:impl:jetMetricsService";

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
    private ConcurrentArrayRingbuffer<Map.Entry<Long, byte[]>> metricsJournal;
    private MetricsConfig config;

    public JetMetricsService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void configure(MetricsConfig config) {
        this.config = config;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        if (config.isEnabled()) {
            int journalSize = Math.max(
                    1, (int) Math.ceil((double) config.getRetentionSeconds() / config.getCollectionIntervalSeconds())
            );
            metricsJournal = new ConcurrentArrayRingbuffer<>(journalSize);
            logger.info("Configuring metrics collection, collection interval=" + config.getCollectionIntervalSeconds()
                    + " seconds and retention=" + config.getRetentionSeconds() + " seconds");
            int[] lastSize = {INITIAL_BUFFER_SIZE};
            JmxRenderer jmxRenderer = new JmxRenderer();
            nodeEngine.getExecutionService().scheduleWithRepetition("MetricsForManCenterCollection", () -> {
                CompressingProbeRenderer renderer = new CompressingProbeRenderer(
                        lastSize[0] * SIZE_FACTOR_NUMERATOR / SIZE_FACTOR_DENOMINATOR);
                this.nodeEngine.getMetricsRegistry().render(renderer);
                this.nodeEngine.getMetricsRegistry().render(jmxRenderer);
                byte[] blob = renderer.getRenderedBlob();
                lastSize[0] = blob.length;
                metricsJournal.add(entry(System.currentTimeMillis(), blob));
                logFine(logger, "Collected %,d metrics, %,d bytes", renderer.getCount(), blob.length);
            }, 1, config.getCollectionIntervalSeconds(), TimeUnit.SECONDS);

        }
    }

    public ConcurrentArrayRingbuffer.RingbufferSlice<Map.Entry<Long, byte[]>> readMetrics(long startSequence) {
        return metricsJournal.copyFrom(startSequence);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    // apply MetricsConfig to HZ properties
    // these properties need to be set here so that the metrics get applied from startup
    public static void applyMetricsConfig(Config hzConfig, MetricsConfig metricsConfig) {
        if (metricsConfig.isEnabled()) {
            hzConfig.setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.INFO.name());
            if (metricsConfig.isEnabledForDataStructures()) {
                hzConfig.setProperty(Diagnostics.METRICS_DISTRIBUTED_DATASTRUCTURES.getName(), "true");
            }
        }
    }
}
