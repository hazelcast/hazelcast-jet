/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.spring;

import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.spring.context.SpringManagedContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"application-context-jet.xml"})
public class TestApplicationContext {

    @Resource(name = "jet-instance")
    private JetInstance jetInstance;

    @Resource(name = "jet-client")
    private JetInstance jetClient;

    @Resource(name = "hazelcast-instance")
    private HazelcastInstance hazelcastInstance;

    @Resource(name = "my-map-bean")
    private IMap map;

    @Resource(name = "my-list-bean")
    private IList list;

    @Resource(name = "my-queue-bean")
    private IQueue queue;

    @BeforeClass
    @AfterClass
    public static void start() {
        Jet.shutdownAll();
    }

    @Test
    public void test() {
        assertNotNull("jetInstance", jetInstance);
        assertNotNull("jetClient", jetClient);
        assertNotNull("hazelcastInstance", hazelcastInstance);
        assertNotNull("map", map);
        assertNotNull("list", list);
        assertNotNull("queue", queue);

        assertJetConfig();
    }

    private void assertJetConfig() {
        JetConfig jetConfig = jetInstance.getConfig();
        Config hazelcastConfig = jetConfig.getHazelcastConfig();
        assertHazelcastConfig(hazelcastConfig);

        InstanceConfig instanceConfig = jetConfig.getInstanceConfig();
        assertEquals(4, instanceConfig.getBackupCount());
        assertEquals(2, instanceConfig.getCooperativeThreadCount());
        assertEquals(200, instanceConfig.getFlowControlPeriodMs());
        assertEquals(1234, instanceConfig.getScaleUpDelayMillis());
        assertFalse(instanceConfig.isLosslessRestartEnabled());

        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        assertEquals(8, edgeConfig.getQueueSize());
        assertEquals(3, edgeConfig.getPacketSizeLimit());
        assertEquals(5, edgeConfig.getReceiveWindowMultiplier());

        assertEquals("bar", jetConfig.getProperties().getProperty("foo"));
    }

    private void assertHazelcastConfig(Config cfg) {
        assertTrue(cfg.getManagedContext() instanceof SpringManagedContext);
        assertEquals("jet-spring", cfg.getClusterName());

        NetworkConfig networkConfig = cfg.getNetworkConfig();
        assertEquals(5707, networkConfig.getPort());
        assertFalse(networkConfig.isPortAutoIncrement());

        JoinConfig join = networkConfig.getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());

        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        assertTrue(tcpIpConfig.isEnabled());
        List<String> members = tcpIpConfig.getMembers();
        assertEquals(1, members.size());
        assertEquals("127.0.0.1:5707", members.get(0));

        assertEquals(3, cfg.getMapConfig("map").getBackupCount());
    }
}
