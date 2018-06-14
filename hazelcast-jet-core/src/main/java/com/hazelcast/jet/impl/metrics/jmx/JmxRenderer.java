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

import com.hazelcast.internal.metrics.MetricsUtil;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.hazelcast.internal.metrics.MetricsUtil.escapeMetricNamePart;

public class JmxRenderer implements ProbeRenderer {

    private final MBeanServer platformMBeanServer;
    private final ConcurrentMap<String, Metric> chm = new ConcurrentHashMap<>();

    public JmxRenderer() {
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    public void renderLong(String name, long value) {
        if (!name.startsWith("[") || !name.endsWith("]")) {
            name = "[metric=" + escapeMetricNamePart(name) + ']';
        }
        final String metricName = name;
        chm.computeIfAbsent(name, k -> {
            HashMap<String, String> map = new HashMap<>();
            Hashtable<String, String> table = new Hashtable<>();
            for (Entry<String, String> entry : MetricsUtil.parseMetricName(metricName)) {
                map.put(entry.getKey(), entry.getValue());
            }

            table.put("type", "Metric");
            try {
                String unit = map.remove("unit");
                Metric bean = new Metric(unit == null ? "unknown" : unit);
                String module = map.remove("module");
                String domain = "com.hazelcast" + (module != null ? "." + module : "");
                String metric = map.remove("metric");
                if (!map.isEmpty()) {
                    String tags = map.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(
                            Collectors.joining(","));
                    table.put("tags", escapeObjectNameValue(tags));
                }
                table.put("name", escapeObjectNameValue(metric));
                platformMBeanServer.registerMBean(bean, new ObjectName(domain, table));
                return bean;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).setValue(value);
    }

    @Override
    public void renderDouble(String name, double value) {

    }

    @Override
    public void renderException(String name, Exception e) {

    }

    @Override
    public void renderNoValue(String name) {

    }

    private String escapeObjectNameValue(String name) {
        if (name.indexOf(',') < 0
                && name.indexOf('=') < 0
                && name.indexOf(':') < 0
                && name.indexOf('\"') < 0
                && name.indexOf('\n') < 0) {
            return name;
        }
        return "\"" + name.replace("\"", "\\\"").replace("\n", "\\n ") + '"';
    }
}
