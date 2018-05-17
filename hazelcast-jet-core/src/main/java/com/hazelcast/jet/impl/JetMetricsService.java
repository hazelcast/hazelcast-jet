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

package com.hazelcast.jet.impl;

import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.util.CompressingProbeRenderer;
import com.hazelcast.jet.impl.util.ConcurrentArrayRingbuffer;
import com.hazelcast.jet.impl.util.ConcurrentArrayRingbuffer.RingbufferSlice;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.Util.entry;

public class JetMetricsService implements ManagedService {

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    // TODO [viliam] make the length configurable
    private final ConcurrentArrayRingbuffer<Map.Entry<Long, byte[]>> metricsBlobs =
            new ConcurrentArrayRingbuffer<>(120);

    public static final String SERVICE_NAME = "hz:impl:jetMetricsService";

    public JetMetricsService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        // if capacity is 0, metric collection is disabled
        if (metricsBlobs.getCapacity() > 0) {
            int[] lastSize = {2 << 16};
            nodeEngine.getExecutionService().scheduleWithRepetition("MetricsForMcCollection", () -> {
                CompressingProbeRenderer renderer = new CompressingProbeRenderer(lastSize[0] * 11 / 10);
                int[] count = {0};
                this.nodeEngine.getMetricsRegistry().render(new ProbeRenderer() {
                    @Override
                    public void renderLong(String name, long value) {
                        count[0]++;
                    }

                    @Override
                    public void renderDouble(String name, double value) {
                        count[0]++;
                    }

                    @Override
                    public void renderException(String name, Exception e) {

                    }

                    @Override
                    public void renderNoValue(String name) {

                    }
                });
                this.nodeEngine.getMetricsRegistry().render(renderer);
                byte[] blob = renderer.getRenderedBlob();
                lastSize[0] = blob.length;
                metricsBlobs.add(entry(System.currentTimeMillis(), blob));
                logger.info("Collected metrics, " + blob.length + " bytes");
            }, 1, 1, TimeUnit.SECONDS);
        }
    }

    public RingbufferSlice<Map.Entry<Long, byte[]>> getMetricBlobs(long startSequence) {
        return metricsBlobs.copyFrom(startSequence);
    }


    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }
}
