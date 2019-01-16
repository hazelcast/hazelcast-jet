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

package com.hazelcast.jet.config;

import com.hazelcast.config.Config;
import com.hazelcast.jet.impl.config.XmlJetConfigLocator;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.jet.config.InstanceConfig.DEFAULT_FLOW_CONTROL_PERIOD_MS;
import static com.hazelcast.jet.config.MetricsConfig.DEFAULT_METRICS_COLLECTION_SECONDS;
import static com.hazelcast.jet.config.MetricsConfig.DEFAULT_METRICS_RETENTION_SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class XmlConfigTest {

    private static final String TEST_XML_1 = "hazelcast-jet-test.xml";
    private static final String TEST_XML_2 = "hazelcast-jet-member-test.xml";
    private static final String TEST_XML_2_GROUP_NAME = "imdg";
    private static final String PASSWORD = "123456";
    private static final String INSTANCE_NAME = "my-instance";

    @Test
    public void when_noConfigSpecified_usesDefaultConfig() {
        // When
        JetConfig jetConfig = JetConfig.loadDefault(new Properties());

        // Then
        assertEquals(Runtime.getRuntime().availableProcessors(),
                jetConfig.getInstanceConfig().getCooperativeThreadCount());
        assertEquals(DEFAULT_FLOW_CONTROL_PERIOD_MS, jetConfig.getInstanceConfig().getFlowControlPeriodMs());

        assertTrue(jetConfig.getMetricsConfig().isEnabled());
        assertTrue(jetConfig.getMetricsConfig().isJmxEnabled());
        assertEquals(DEFAULT_METRICS_RETENTION_SECONDS, jetConfig.getMetricsConfig().getRetentionSeconds());
        assertEquals(DEFAULT_METRICS_COLLECTION_SECONDS, jetConfig.getMetricsConfig().getCollectionIntervalSeconds());
        assertFalse(jetConfig.getMetricsConfig().isMetricsForDataStructuresEnabled());

        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_filePathSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_1);
            os.write(Util.readFully(resourceAsStream));
        }

        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, tempFile.getAbsolutePath());

        // When
        JetConfig jetConfig = JetConfig.loadDefault(properties);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_filePathMemberSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("imdg", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_2);
            os.write(Util.readFully(resourceAsStream));
        }

        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_MEMBER_CONFIG_PROPERTY, tempFile.getAbsolutePath());

        // When
        JetConfig jetConfig = JetConfig.loadDefault(properties);

        // Then
        assertXmlMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_classpathSpecified_usesSpecifiedResource() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + TEST_XML_1);

        // When
        JetConfig jetConfig = JetConfig.loadDefault(properties);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_classpathMemberSpecified_usesSpecifiedResource() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_MEMBER_CONFIG_PROPERTY, "classpath:" + TEST_XML_2);

        // When
        JetConfig jetConfig = JetConfig.loadDefault(properties);

        // Then
        assertXmlMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_configHasVariable_variablesAreReplaced() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, "classpath:hazelcast-jet-with-variables.xml");
        properties.put("thread.count", String.valueOf(55));
        properties.put("flow.control.period", "50");
        properties.put("backup.count", "2");
        properties.put("scale.up.delay.millis", "1234");
        properties.put("metrics.enabled", "false");
        properties.put("metrics.jmxEnabled", "false");
        properties.put("metrics.retention", "124");
        properties.put("metrics.collection-interval", "123");
        properties.put("metrics.enabled-for-data-structures", "true");

        // When
        JetConfig jetConfig = JetConfig.loadDefault(properties);

        // Then
        assertConfig(jetConfig);
    }

    @Ignore("To confirm if substitution in Jet should be visible to IMDG")
    @Test
    public void when_configMemberHasVariable_variablesAreReplaced() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_MEMBER_CONFIG_PROPERTY, "classpath:${my.filename}");
        properties.put("my.filename", TEST_XML_2);
        properties.put("imdg.pass", PASSWORD);
        properties.put("imdg.instance.name", INSTANCE_NAME);

        // When
        JetConfig jetConfig = JetConfig.loadDefault(properties);

        // Then
        assertXmlMemberConfig(jetConfig.getHazelcastConfig());
        assertThat(jetConfig.getHazelcastConfig().getGroupConfig().getPassword(), equalTo(PASSWORD));
        assertThat(jetConfig.getHazelcastConfig().getInstanceName(), equalTo(INSTANCE_NAME));
    }

    @Test
    public void when_edgeDefaultsSpecified_usesSpecified() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + TEST_XML_1);

        // When
        JetConfig jetConfig = JetConfig.loadDefault(properties);

        // Then
        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        assertEquals("queueSize", 999, edgeConfig.getQueueSize());
        assertEquals("packetSizeLimit", 997, edgeConfig.getPacketSizeLimit());
        assertEquals("receiveWindowMultiplier", 996, edgeConfig.getReceiveWindowMultiplier());
    }

    private static void assertConfig(JetConfig jetConfig) {
        assertEquals("cooperativeThreadCount", 55, jetConfig.getInstanceConfig().getCooperativeThreadCount());
        assertEquals("backupCount", 2, jetConfig.getInstanceConfig().getBackupCount());
        assertEquals("flowControlMs", 50, jetConfig.getInstanceConfig().getFlowControlPeriodMs());
        assertEquals("scaleUpDelayMillis", 1234, jetConfig.getInstanceConfig().getScaleUpDelayMillis());

        assertEquals("value1", jetConfig.getProperties().getProperty("property1"));
        assertEquals("value2", jetConfig.getProperties().getProperty("property2"));

        MetricsConfig metricsCfg = jetConfig.getMetricsConfig();
        assertFalse("isEnabled", metricsCfg.isEnabled());
        assertFalse("isJmxEnabled", metricsCfg.isJmxEnabled());
        assertEquals("metricsRetentionSeconds", 124, metricsCfg.getRetentionSeconds());
        assertEquals("metricsCollectionInterval", 123, metricsCfg.getCollectionIntervalSeconds());
        assertTrue("metricsForDataStructures", metricsCfg.isMetricsForDataStructuresEnabled());
    }

    private static void assertDefaultMemberConfig(Config config) {
        assertThat(config, not(nullValue()));
        assertThat(config.getGroupConfig().getName(), not(equalTo(TEST_XML_2_GROUP_NAME)));
    }

    private static void assertXmlMemberConfig(Config config) {
        assertThat(config, not(nullValue()));
        assertThat(config.getGroupConfig().getName(), equalTo(TEST_XML_2_GROUP_NAME));
    }
}
