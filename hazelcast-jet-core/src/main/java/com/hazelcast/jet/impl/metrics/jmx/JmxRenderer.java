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
import com.hazelcast.jet.impl.metrics.MetricsRenderPlugin;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

/**
 * Renderer to create, register and unregister mBeans for metrics as they are
 * rendered.
 */
public class JmxRenderer implements MetricsRenderPlugin {

    private final MBeanServer platformMBeanServer;
    private final String instanceNameEscaped;

    /** key: metric name, value: MetricData */
    private final Map<String, MetricData> metricNameToMetricData = new HashMap<>();
    /** key: jmx object name, value: mBean */
    private final Map<ObjectName, MetricsMBean> mBeans = new HashMap<>();

    public JmxRenderer(String instanceName) {
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        instanceNameEscaped = escapeObjectNameValue(instanceName);
    }

    @Override
    public void renderLong(String metricName, long value) {
        renderNumber(metricName, value);
    }

    @Override
    public void renderDouble(String metricName, double value) {
        renderNumber(metricName, value);
    }

    private void renderNumber(String metricName, Number value) {
        MetricData metricData = metricNameToMetricData.computeIfAbsent(metricName,
                n -> new MetricData(n, instanceNameEscaped));
        assert !metricData.wasPresent : "metric '" + metricName + "' was rendered twice";
        metricData.wasPresent = true;
        MetricsMBean mBean = mBeans.computeIfAbsent(metricData.objectName, objectName -> {
            MetricsMBean bean = new MetricsMBean();
            try {
                platformMBeanServer.registerMBean(bean, objectName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return bean;
        });
        mBean.setMetricValue(metricData.metric, metricData.unit, value);
    }

    @Override
    public void onRenderingComplete() {
        // remove metrics that weren't present in current rendering
        for (Iterator<MetricData> iterator = metricNameToMetricData.values().iterator(); iterator.hasNext(); ) {
            MetricData metricData = iterator.next();
            if (!metricData.wasPresent) {
                iterator.remove();
                // remove the metric from the bean
                MetricsMBean mBean = mBeans.get(metricData.objectName);
                mBean.removeMetric(metricData.metric);
                // remove entire bean if no metric is left
                if (mBean.numAttributes() == 0) {
                    mBeans.remove(metricData.objectName);
                    try {
                        platformMBeanServer.unregisterMBean(metricData.objectName);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } else {
                metricData.wasPresent = false;
            }
        }
    }

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private static String escapeObjectNameValue(String name) {
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

    private static class MetricData {
        ObjectName objectName;
        String metric;
        String unit;
        boolean wasPresent;

        @SuppressWarnings("checkstyle:ExecutableStatementCount")
        MetricData(String metricName, String instanceNameEscaped) {
            // The code below converts metric name in two formats (old and new)
            // into a JMX object name that, when rendered in tools like Java
            // Mission Control or JConsole, will have nice tree structure.
            //
            // Examples of metric names in old format:
            // - classloading.loadedClassesCount
            // - classloading.totalLoadedClassesCount
            // - client.endpoint.count
            // - operation.thread[hz._hzInstance_2_jet.generic-operation.thread-<N>].errorCount
            //
            // Old metric names will be split on each '.' (ignoring periods inside of '[]').
            //
            // Examples of metric names in new format:
            // - [module=jet,job=<jobId>,vertex=<vertexId>,proc=<processorNum>,metric=queuesCapacity]
            // - [module=jet,job=<jobId>,vertex=<vertexId>,proc=<processorNum>,metric=queuesSize]
            // - [module=jet,job=<jobId>,vertex=<vertexId>,ordinal=N,metric=distributedBytesIn]
            //
            // New metric names will be split on each tag, preserving the tag order.
            //
            // `metric` tag is treated specially: it will be removed from metric name and multiple metrics
            // with the same tags except `metric` will be grouped under one MBean as its attributes. For old
            // format, the part after the last '.' is treated as metric name.
            //
            // The JMX Object Name has special structure:
            //   <domain>:<tag1>=<value1>,<tag2>=<value2>,...
            // The UI tools display this hierarchy:
            //   <domain>/<value1>/<value2>/...
            //
            // Tag order is irrelevant to ObjectName identity, but is important for the UI: it builds the tree
            // based on the tag order. That's why we have to use the `ObjectName(String)` constructor;
            // the ObjectName(String, Hashtable) constructor doesn't preserve the order. For tags we simply
            // use "tag1", "tag2" because the tag name is not displayed in the UI.

            List<Entry<String, String>> tagsList = metricName.startsWith("[") && metricName.endsWith("]")
                    ? MetricsUtil.parseMetricName(metricName)
                    : parseOldMetricName(metricName);
            StringBuilder mBeanTags = new StringBuilder();
            String module = null;
            metric = "metric";
            unit = "unknown";
            int tag = 0;

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
                        mBeanTags.append("tag")
                                 .append(tag++)
                                 .append('=');
                        if (entry.getKey().length() == 0) {
                            // key is empty for old metric names (see parseOldMetricName)
                            mBeanTags.append(escapeObjectNameValue(entry.getValue()));
                        } else {
                            // We add both key and value to the value: the key carries semantic value, we
                            // want to see it in the UI.
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
            try {
                objectName = new ObjectName(objectNameStr.toString());
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
            }
        }

        private static List<Entry<String, String>> parseOldMetricName(String name) {
            List<Entry<String, String>> res = new ArrayList<>();
            boolean inBracket = false;
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < name.length(); i++) {
                char ch = name.charAt(i);
                if (ch == '.' && !inBracket) {
                    if (builder.length() > 0) {
                        res.add(entry("", builder.toString()));
                    }
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
            // the trailing entry will be marked as `metric` tag
            if (builder.length() > 0) {
                res.add(entry("metric", builder.toString()));
            }
            return res;
        }
    }
}
