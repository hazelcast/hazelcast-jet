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

package com.hazelcast.jet.impl.metrics.jmx;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;

import java.util.HashMap;
import java.util.Map;

// This class must be named like this to follow the "Standard MBean" contract
public class Metrics implements MetricsMBean {
    private final MetricsRegistry metricsRegistry;

    public Metrics(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
    }

    @Override
    public Map<String, Object> getMetrics() {
        Renderer renderer = new Renderer();
        metricsRegistry.render(renderer);
        return renderer.map;
    }

    private static class Renderer implements ProbeRenderer {

        private Map<String, Object> map = new HashMap<>();

        @Override
        public void renderLong(String name, long value) {
            map.put(name, value);
        }

        @Override
        public void renderDouble(String name, double value) {
            map.put(name, value);
        }

        @Override
        public void renderException(String name, Exception e) {
            map.put(name, e.toString());
        }

        @Override
        public void renderNoValue(String name) {
            map.put(name, null);
        }
    }
}
