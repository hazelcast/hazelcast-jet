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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricsUtil;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.jet.Util.entry;

public class JmxRenderer2 implements ProbeRenderer {

    private final MBeanServer platformMBeanServer;
    private final ConcurrentMap<String, MetricDynamicMBean> chm = new ConcurrentHashMap<>();
    private final String instanceNameEscaped;

    public JmxRenderer2(HazelcastInstance instance) {
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        instanceNameEscaped = escapeObjectNameValue(instance.getName());
    }

    @Override
    public void renderLong(String name, long value) {
        Entry<String, String> entry = toObjectNameAndMetric(name);
        chm.computeIfAbsent(entry.getKey(), v -> {
            MetricDynamicMBean bean = new MetricDynamicMBean();
            try {
                platformMBeanServer.registerMBean(bean, new ObjectName(entry.getKey()));
                return bean;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).addMetric(entry.getValue(), value);
    }

    private Entry<String, String> toObjectNameAndMetric(String name) {
        List<Entry<String, String>> tagsList = name.startsWith("[") && name.endsWith("]")
                ? MetricsUtil.parseMetricName(name)
                : parseOldMetricName(name);
        StringBuilder mBeanTags = new StringBuilder();
        String module = null;
        String metric = null;
        int tag = 0;

        for (Entry<String, String> entry : tagsList) {
            switch (entry.getKey()) {
                case "unit":
                    break;
                case "module":
                    module = entry.getValue();
                    break;
                case "metric":
                    metric = entry.getValue();
                    break;
                default:
                    if (mBeanTags.length() > 0) {
                        mBeanTags.append(',');
                    }
                    mBeanTags.append("tag")
                             .append(tag++)
                             .append('=');
                    if (entry.getKey().length() == 0) {
                        // key is empty for old metric names (see parseOldMetricName)
                        mBeanTags.append(escapeObjectNameValue(entry.getValue()));
                    } else {
                        mBeanTags.append(escapeObjectNameValue(entry.getKey() + '=' + entry.getValue()));
                    }
            }
        }
        assert metric != null : "metric == null";

        StringBuilder objectNameStr = new StringBuilder(mBeanTags.length() + (1 << 3 << 3));
        objectNameStr.append("com.hazelcast");
        if (module != null) {
            objectNameStr.append('.').append(module);
        }
        objectNameStr.append(":type=Metrics");
        objectNameStr.append(",instance=").append(instanceNameEscaped);
        if (mBeanTags.length() > 0) {
            objectNameStr.append(',').append(mBeanTags);
        }
        String objectName = objectNameStr.toString();
        return entry(objectName, metric);
    }

    private static List<Entry<String, String>> parseOldMetricName(String name) {
        List<Entry<String, String>> res = new ArrayList<>();
        boolean inBracket = false;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (ch == '.' && !inBracket) {
                res.add(entry("", builder.toString()));
                builder.setLength(0);
            } else {
                builder.append(ch);
                if (ch == '[') {
                    inBracket = true;
                } else if (ch == ']') {
                    inBracket = false;
                }
            }
        }
        // trailing entry
        if (builder.length() > 0) {
            res.add(entry("metric", builder.toString()));
        }
        return res;
    }

    @Override
    public void renderDouble(String name, double value) {

    }

    @Override
    public void renderException(String name, Exception e) {
        e.printStackTrace();
    }

    @Override
    public void renderNoValue(String name) {

    }

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private String escapeObjectNameValue(String name) {
        if (name.indexOf(',') < 0
                && name.indexOf('=') < 0
                && name.indexOf(':') < 0
                && name.indexOf('*') < 0
                && name.indexOf('?') < 0
                && name.indexOf('\"') < 0
                && name.indexOf('\n') < 0) {
            return name;
        }
        return "\"" + name.replace("\"", "\\\"").replace("\n", "\\n ") + '"';
    }
}
