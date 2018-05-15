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
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class JetMetricsService {

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    // TODO [viliam] make the length configurable
    private final ConcurrentArrayRingbuffer<Tuple2<Long, byte[]>> metricsBlobs = new ConcurrentArrayRingbuffer<>(120);

    public JetMetricsService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());

        // if capacity is 0, metric collection is disabled
        if (metricsBlobs.getCapacity() > 0) {
            int[] lastSize = {2 << 16};
            nodeEngine.getExecutionService().scheduleWithRepetition("MetricsForMcCollection", () -> {
                CompressingProbeRenderer renderer = new CompressingProbeRenderer(lastSize[0] * 11 / 10);
                int[] count = {0};
                long start = System.nanoTime();
                this.nodeEngine.getMetricsRegistry().render(new ProbeRenderer() {
                    @Override
                    public void renderLong(String name, long value) {
//                        if (name.startsWith("jet.")) {
//                            logger.info(name + "=" + value);
//                        }
                        count[0]++;
                    }

                    @Override
                    public void renderDouble(String name, double value) {
//                        if (name.startsWith("jet.")) {
//                            logger.info(name + "=" + value);
//                        }
                        count[0]++;
                    }

                    @Override
                    public void renderException(String name, Exception e) {

                    }

                    @Override
                    public void renderNoValue(String name) {

                    }
                });
                logger.info("aaa, " + NANOSECONDS.toMillis(System.nanoTime() - start));
                this.nodeEngine.getMetricsRegistry().render(renderer);
                byte[] blob = renderer.getRenderedBlob();
                lastSize[0] = blob.length;
                logger.info("bbb, count=" + count[0]);
                metricsBlobs.add(tuple2(System.currentTimeMillis(), blob));
                logger.info("Collected metrics, " + blob.length + " bytes");
            }, 1, 1, TimeUnit.SECONDS);
        }
    }

    public RingbufferSlice<Tuple2<Long, byte[]>> getMetricBlobs(long startSequence) {
        return metricsBlobs.copyFrom(startSequence);
    }
}
