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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.map.helpers.AMapStore;
import com.hazelcast.collection.IList;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jms.ConnectionFactory;
import javax.jms.TextMessage;
import java.io.Serializable;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.junit.Assert.assertEquals;

public class JmsIntegration_NonSharedClusterTest extends JetTestSupport {

    @ClassRule
    public static EmbeddedActiveMQResource resource = new EmbeddedActiveMQResource();

    private static volatile boolean storeFailed;

    @Test
    public void test() throws Exception {
        JetInstance instance1 = createJetMember();
        JetInstance instance2 = createJetMember();

        // use higher number of messages so that each of the parallel processors gets some
        int messageCount = 10_000;
        JmsTestUtil.sendMessages(getConnectionFactorySupplier(false).get(), "queue", true, messageCount);

        Pipeline p = Pipeline.create();
        IList<String> sinkList = instance1.getList("sinkList");
        p.readFrom(Sources.jmsQueueBuilder(getConnectionFactorySupplier(true))
                          .destinationName("queue")
                          .build(msg -> ((TextMessage) msg).getText()))
         .withoutTimestamps()
         .writeTo(Sinks.list(sinkList));

        instance1.newJob(p, new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(DAYS.toMillis(1)));

        assertTrueEventually(() -> assertEquals("expected items not in sink", messageCount, sinkList.size()));

        // Now forcefully shut down one of the members. The job should restart and immediately
        // re-emit all the messages. If the transaction from the other isn't rolled back, those
        // messages will be stalled in the unfinished transaction until it is rolled back by Artemis
        // after the default 5 minutes.
        instance2.getHazelcastInstance().getLifecycleService().terminate();
        assertTrueEventually(() -> assertEquals("items should be emitted twice", messageCount * 2, sinkList.size()), 20);
    }

    // TODO [viliam] snapshot failure should be ignored if guarantee != ex-once

    @Test
    public void when_snapshotFails_then_jobFails() {
        storeFailed = false;
        // force snapshots to fail by adding a failing map store configuration for snapshot data maps
        JetConfig config = new JetConfig();
        MapConfig mapConfig = new MapConfig(JobRepository.SNAPSHOT_DATA_MAP_PREFIX + '*');
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new FailingMapStore());
        config.getHazelcastConfig().addMapConfig(mapConfig);

        JetInstance instance = createJetMember(config);
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jmsQueue(getConnectionFactorySupplier(true), "queue"))
         .withoutTimestamps()
         .writeTo(Sinks.noop());

        Job job = instance.newJob(p, new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(100));

        try {
            job.join();
        } catch (Exception e) {
            assertContains(e.getMessage(), "snapshot failed, can't continue to use the transaction");
        }
    }

    private static SupplierEx<ConnectionFactory> getConnectionFactorySupplier(boolean xa) {
        return () -> xa
                ? new ActiveMQXAConnectionFactory(resource.getVmURL())
                : new ActiveMQConnectionFactory(resource.getVmURL());
    }

    private static class FailingMapStore extends AMapStore implements Serializable {
        @Override
        public void store(Object o, Object o2) {
            storeFailed = true;
            throw new UnsupportedOperationException("failing map store");
        }
    }
}
