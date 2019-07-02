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

package com.hazelcast.jet.impl;

import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.jet.impl.execution.ExecutionContext;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class JobMetricsUtil {

    private JobMetricsUtil() {
    }

    public static Map<String, Long> getJobMetrics(JetService service, long executionId) {
        ExecutionContext executionContext = service.getJobExecutionService().getExecutionContext(executionId);
        if (executionContext == null) {
            return Collections.emptyMap();
        } else {
            ToMapProbeRenderer renderer = new ToMapProbeRenderer();
            executionContext.renderJobMetrics(renderer);
            return renderer.toMap();
        }
    }

    static Map<String, Long> mergeMetrics(Collection<Object> metrics) {
        Map<String, Long> mergedMetrics = new HashMap<>();
        for (Object o : metrics) {
            mergedMetrics.putAll((Map<String, Long>) o);
        }
        return mergedMetrics;
    }

    private static class ToMapProbeRenderer implements ProbeRenderer {

        // required precision after the decimal point for doubles
        private static final int CONVERSION_PRECISION = 4;
        // coefficient for converting doubles to long
        private static final double DOUBLE_TO_LONG = Math.pow(10, CONVERSION_PRECISION);

        private final Map<String, Long> map = new HashMap<>();

        public Map<String, Long> toMap() {
            return Collections.unmodifiableMap(map);
        }

        @Override
        public void renderLong(String name, long value) {
            map.put(name, value);
        }

        @Override
        public void renderDouble(String name, double value) {
            long longValue = Math.round(value * DOUBLE_TO_LONG);
            renderLong(name, longValue);
        }

        @Override
        public void renderException(String name, Exception e) {

        }

        @Override
        public void renderNoValue(String name) {

        }
    }

}
