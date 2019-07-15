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

import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.JobMetrics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class JobMetricsUtil {

    private static final Pattern METRIC_KEY_EXEC_ID_PATTERN =
            Pattern.compile("\\[module=jet,job=[^,]+,exec=([^,]+),.*");

    // required precision after the decimal point for doubles
    private static final int CONVERSION_PRECISION = 4;
    // coefficient for converting doubles to long
    private static final double DOUBLE_TO_LONG = Math.pow(10, CONVERSION_PRECISION);

    private JobMetricsUtil() {
    }

    public static Long getExecutionIdFromMetricName(String metricName) {
        Matcher m = METRIC_KEY_EXEC_ID_PATTERN.matcher(metricName);
        if (!m.matches()) {
            // not a job-related metric, ignore it
            return null;
        }
        return Util.idFromString(m.group(1));
    }

    public static long toLongMetricValue(double value) {
        return Math.round(value * DOUBLE_TO_LONG);
    }

    static JobMetrics mergeMetrics(Collection<Object> metrics) {
        Map<String, Long> map = new HashMap<>();
        for (Object o : metrics) {
            map.putAll(((JobMetrics) o).asMap());
        }
        return JobMetrics.of(map);
    }
}
