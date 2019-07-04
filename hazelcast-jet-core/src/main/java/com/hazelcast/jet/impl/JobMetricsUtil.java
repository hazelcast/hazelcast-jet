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

import com.hazelcast.internal.metrics.MetricsUtil;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.JobMetrics;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class JobMetricsUtil {

    // required precision after the decimal point for doubles
    private static final int CONVERSION_PRECISION = 4;
    // coefficient for converting doubles to long
    private static final double DOUBLE_TO_LONG = Math.pow(10, CONVERSION_PRECISION);

    private JobMetricsUtil() {
    }

    public static Long getExecutionIdFromMetricName(String metricName) {
        if (!metricName.startsWith("[") || !metricName.endsWith("]")) {
            //non-standard name format, it's not a job metric
            return null;
        }

        List<Map.Entry<String, String>> tagEntries = MetricsUtil.parseMetricName(metricName);

        //attempt to identify the execution id tag in a hacky, but fast way
        if (tagEntries.size() >= 3 && "exec".equals(tagEntries.get(2).getKey())) {
            return Util.idFromString(tagEntries.get(2).getValue());
        }

        //fall back to full search
        for (Map.Entry<String, String> entry : tagEntries) {
            if ("exec".equals(entry.getKey())) {
                return Util.idFromString(entry.getValue());
            }
        }

        return null;
    }

    public static long toLongMetricValue(double value) {
        return Math.round(value * DOUBLE_TO_LONG);
    }

    static JobMetrics mergeMetrics(Collection<Object> metrics) {
        JobMetrics mergedMetrics = JobMetrics.EMPTY;
        for (Object o : metrics) {
            mergedMetrics = mergedMetrics.merge((JobMetrics) o);
        }
        return mergedMetrics;
    }
}
