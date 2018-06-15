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

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toCollection;

public class MetricDynamicMBean implements DynamicMBean {

    private final Map<String, Long> metrics = new HashMap<>();

    void addMetric(String name, long value) {
        metrics.put(name, value);
    }

    @Override
    public Object getAttribute(String attribute) {
        return metrics.get(attribute);
    }

    @Override
    public void setAttribute(Attribute attribute) {
        throw new UnsupportedOperationException("setting attributes is not supported");
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = Arrays.stream(attributes)
                .map(a -> new Attribute(a, getAttribute(a)))
                .collect(toCollection(AttributeList::new));
        return list;
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setting attributes is not supported");
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) {
        throw new UnsupportedOperationException("invoking is not supported");
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        return new MBeanInfo("Metric", "", attributeInfos(), null, null, null, null);
    }

    private MBeanAttributeInfo[] attributeInfos() {
        MBeanAttributeInfo[] array = new MBeanAttributeInfo[metrics.size()];
        int i = 0;
        for (String metric : metrics.keySet()) {
            array[i++] = new MBeanAttributeInfo(metric, "", "", true, false, false);
        }

        return array;
    }

}
