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

package com.hazelcast.jet.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URL;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ClasspathXmlJetConfigTest {

    private static final String TEST_XML_JET = "hazelcast-jet-test.xml";
    private static final String TEST_XML_JET_WITH_VARIABLES = "hazelcast-jet-with-variables.xml";
    private static final String TEST_XML_MEMBER = "hazelcast-jet-member-test.xml";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_bothNull_then_default() {
        JetConfig config = new ClasspathXmlJetConfig(null, null);
        assertNotNull(config);
    }

    @Test
    public void when_botNonNull_then_loaded() {
        JetConfig config = new ClasspathXmlJetConfig(TEST_XML_JET, TEST_XML_MEMBER);
        assertEquals("imdg", config.getHazelcastConfig().getGroupConfig().getName());
        assertEquals(55, config.getInstanceConfig().getCooperativeThreadCount());
    }

    @Test
    public void when_jetConfigOnly_then_loaded() {
        JetConfig config = new ClasspathXmlJetConfig(TEST_XML_JET, null);
        assertEquals(55, config.getInstanceConfig().getCooperativeThreadCount());
    }

    @Test
    public void when_memberConfigOnly_then_loaded() {
        JetConfig config = new ClasspathXmlJetConfig(null, TEST_XML_MEMBER);
        assertEquals("imdg", config.getHazelcastConfig().getGroupConfig().getName());
    }

    @Test
    public void when_wrongName_then_fail() {
        exception.expect(IllegalArgumentException.class);
        new ClasspathXmlJetConfig("foobar", "foobar");
    }

    @Test
    public void when_customClassLoader_then_used() {
        ClassLoader testCL = new ClassLoader() {
            @Override
            protected URL findResource(String name) {
                if ("member".equals(name)) {
                    return Thread.currentThread().getContextClassLoader().getResource(TEST_XML_MEMBER);
                }
                return super.findResource(name);
            }
        };

        new ClasspathXmlJetConfig(testCL, null, "member");
    }

    @Test
    public void when_customProperties_then_used() {
        Properties properties = new Properties();
        properties.setProperty("thread.count", "123");
        properties.setProperty("flow.control.period", "456");
        properties.setProperty("backup.count", "6");

        JetConfig config = new ClasspathXmlJetConfig(TEST_XML_JET_WITH_VARIABLES, null, properties);
        assertEquals(123, config.getInstanceConfig().getCooperativeThreadCount());
        assertEquals(456, config.getInstanceConfig().getFlowControlPeriodMs());
        assertEquals(6, config.getInstanceConfig().getBackupCount());
    }
}
