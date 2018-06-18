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

import com.hazelcast.jet.datamodel.Tuple2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmxRendererTest {

    private static final ObjectName OBJECT_NAME_NO_MODULE;
    private static final String MODULE_NAME = "moduleA";
    private static final ObjectName OBJECT_NAME_WITH_MODULE;

    static {
        try {
            OBJECT_NAME_NO_MODULE = new ObjectName("com.hazelcast:*");
            OBJECT_NAME_WITH_MODULE = new ObjectName("com.hazelcast." + MODULE_NAME + ":*");
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    private JmxRenderer jmxRenderer;
    private MBeanServer platformMBeanServer;

    @Before
    public void before() throws Exception {
        jmxRenderer = new JmxRenderer("inst1");
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            assertMBeans(emptyList());
        } catch (AssertionError e) {
            throw new AssertionError("JMX beans used by this test are already registered", e);
        }
    }

    @After
    public void after() throws Exception {
        // unregister all beans in domains used in the test to not interfere with other tests
        try {
            for (ObjectInstance instance : queryOurInstances()) {
                platformMBeanServer.unregisterMBean(instance.getObjectName());
            }
        } catch (InstanceNotFoundException ignored) { }
    }

    @Test
    public void when_singleMetricOldFormat() throws Exception {
        jmxRenderer.renderLong("a.b.c", 1L);
        assertMBeans(singletonList(
                tuple2("com.hazelcast:type=Metrics,instance=inst1,tag0=a,tag1=b", singletonList(entry("c", 1L)))));
    }

    @Test
    public void when_trailingPeriodOldFormat_doNotFail() throws Exception {
        jmxRenderer.renderLong("a.b.", 1L);
        assertMBeans(singletonList(
                tuple2("com.hazelcast:type=Metrics,instance=inst1,tag0=a,tag1=b", singletonList(entry("metric", 1L)))));
    }

    @Test
    public void when_twoDotsOldFormat_doNotFail() throws Exception {
        jmxRenderer.renderLong("a.b..c", 1L);
        assertMBeans(singletonList(
                tuple2("com.hazelcast:type=Metrics,instance=inst1,tag0=a,tag1=b", singletonList(entry("c", 1L)))));
    }

    @Test
    public void when_metricTagMissingNewFormat_then_useDefault() throws Exception {
        jmxRenderer.renderLong("a.b.", 1L);
        assertMBeans(singletonList(
                tuple2("com.hazelcast:type=Metrics,instance=inst1,tag0=a,tag1=b", singletonList(entry("metric", 1L)))));
    }

    @Test
    public void when_singleMetricNewFormat() throws Exception {
        jmxRenderer.renderLong("[tag1=a,tag2=b,metric=c]", 1L);
        assertMBeans(singletonList(
                tuple2("com.hazelcast:type=Metrics,instance=inst1,tag0=\"tag1=a\",tag1=\"tag2=b\"",
                        singletonList(entry("c", 1L)))));
    }

    @Test
    public void when_singleMetricNewFormatWithModule() throws Exception {
        jmxRenderer.renderLong("[tag1=a,module=" + MODULE_NAME + ",metric=c]", 1L);
        assertMBeans(singletonList(
                tuple2("com.hazelcast." + MODULE_NAME + ":type=Metrics,instance=inst1,tag0=\"tag1=a\"",
                        singletonList(entry("c", 1L)))));
    }

    @Test
    public void when_moreMetricsOldFormat() throws Exception {
        jmxRenderer.renderLong("a.b.c", 1L);
        jmxRenderer.renderLong("a.b.d", 2L);
        jmxRenderer.renderLong("a.c.a", 3L);
        jmxRenderer.renderLong("a", 4L);
        assertMBeans(asList(
                tuple2("com.hazelcast:type=Metrics,instance=inst1,tag0=a,tag1=b",
                        asList(entry("c", 1L), entry("d", 2L))),
                tuple2("com.hazelcast:type=Metrics,instance=inst1,tag0=a,tag1=c",
                        singletonList(entry("a", 3L))),
                tuple2("com.hazelcast:type=Metrics,instance=inst1",
                        singletonList(entry("a", 4L)))
        ));
    }

    @Test
    public void when_moreMetricsNewFormat() throws Exception {
        jmxRenderer.renderLong("[tag1=a,tag2=b,metric=c]", 1L);
        jmxRenderer.renderLong("[tag1=a,tag2=b,metric=d]", 2L);
        jmxRenderer.renderLong("[module=" + MODULE_NAME + ",tag1=a,tag2=b,metric=d]", 5L);
        jmxRenderer.renderLong("[tag1=a,tag2=c,metric=a]", 3L);
        jmxRenderer.renderLong("[metric=a]", 4L);
        assertMBeans(asList(
                tuple2("com.hazelcast:type=Metrics,instance=inst1,tag0=\"tag1=a\",tag1=\"tag2=b\"",
                        asList(entry("c", 1L), entry("d", 2L))),
                tuple2("com.hazelcast." + MODULE_NAME + ":type=Metrics,instance=inst1,tag0=\"tag1=a\",tag1=\"tag2=b\"",
                        singletonList(entry("d", 5L))),
                tuple2("com.hazelcast:type=Metrics,instance=inst1,tag0=\"tag1=a\",tag1=\"tag2=c\"",
                        singletonList(entry("a", 3L))),
                tuple2("com.hazelcast:type=Metrics,instance=inst1",
                        singletonList(entry("a", 4L)))
        ));
    }

    private void assertMBeans(List<Tuple2<String, List<Entry<String, Number>>>> expected) throws Exception {
        Set<ObjectInstance> instances = queryOurInstances();
        Map<ObjectName, ObjectInstance> instanceMap =
                instances.stream().collect(Collectors.toMap(ObjectInstance::getObjectName, Function.identity()));

        assertEquals("actual: " + instances, expected.size(), instances.size());

        for (Tuple2<String, List<Entry<String, Number>>> entry : expected) {
            ObjectName on = new ObjectName(entry.f0());
            assertTrue("name: " + on + " not in instances " + instances, instanceMap.containsKey(on));
            for (Entry<String, Number> attribute : entry.f1()) {
                assertEquals("Attribute '" + attribute.getKey() + "' of '" + on + "' doesn't match",
                        attribute.getValue(), platformMBeanServer.getAttribute(on, attribute.getKey()));
            }
        }
    }

    private Set<ObjectInstance> queryOurInstances() {
        Set<ObjectInstance> instances = new HashSet<>();
        instances.addAll(platformMBeanServer.queryMBeans(OBJECT_NAME_NO_MODULE, null));
        instances.addAll(platformMBeanServer.queryMBeans(OBJECT_NAME_WITH_MODULE, null));
        return instances;
    }
}
