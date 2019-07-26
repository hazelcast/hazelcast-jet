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

import com.hazelcast.core.Member;
import com.hazelcast.internal.metrics.MetricsUtil;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.MetricTags;

import javax.annotation.Nonnull;
import java.util.Objects;
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

    public static Long getExecutionIdFromMetricName(@Nonnull String metricName) {
        Objects.requireNonNull(metricName, "metricName");

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

    public static String getMemberPrefix(@Nonnull Member member) {
        Objects.requireNonNull(member, "member");

        String uuid = member.getUuid();
        String address = member.getAddress().toString();
        return MetricTags.MEMBER + "=" + MetricsUtil.escapeMetricNamePart(uuid) + "," +
                MetricTags.ADDRESS + "=" + MetricsUtil.escapeMetricNamePart(address) + ",";
    }

    public static String addPrefixToName(@Nonnull String name, @Nonnull String prefix) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(prefix, "prefix");

        if (prefix.isEmpty() || !prefix.endsWith(",")) {
            throw new IllegalArgumentException("Invalid prefix");
        }

        if (name.length() < 3 || !name.startsWith("[") || !name.endsWith("]")) {
            throw new IllegalArgumentException("Invalid name format");
        }

        return "[" + prefix + name.substring(1);
    }
}
