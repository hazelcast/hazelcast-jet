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
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.jet.Util.entry;

public class JmxRenderer implements ProbeRenderer {

    private final MBeanServer platformMBeanServer;
    private final ConcurrentMap<String, Metric> chm = new ConcurrentHashMap<>();

    public JmxRenderer() {
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    public void renderLong(String name, long value) {
        chm.computeIfAbsent(name, k -> {
            List<Entry<String, String>> tagsList = name.startsWith("[") && name.endsWith("]")
                    ? MetricsUtil.parseMetricName(name)
                    : Collections.singletonList(entry("metric", name));
            StringBuilder mBeanTags = new StringBuilder();
            String unit = "unknown";
            String module = null;
            String metric = null;

            for (Entry<String, String> entry : tagsList) {
                switch (entry.getKey()) {
                    case "unit":
                        unit = entry.getValue();
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
                        mBeanTags.append(MetricsUtil.escapeMetricNamePart(entry.getKey()))
                                 .append('=')
                                 .append(MetricsUtil.escapeMetricNamePart(entry.getValue()));
                }
            }
            assert metric != null : "metric == null";

            StringBuilder objectNameStr = new StringBuilder(mBeanTags.length() + (1 << 3 << 3));
            objectNameStr.append("com.hazelcast");
            if (module != null) {
                objectNameStr.append('.').append(module);
            }
            objectNameStr.append(":type=Metrics");
            objectNameStr.append(",name=").append(escapeObjectNameValue(metric));
            if (mBeanTags.length() > 0) {
                objectNameStr.append(",tags=")
                        .append(escapeObjectNameValue(mBeanTags.toString()));
            }

            Metric bean = new Metric(unit);
            try {
                platformMBeanServer.registerMBean(bean, new ObjectName(objectNameStr.toString()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return bean;
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
