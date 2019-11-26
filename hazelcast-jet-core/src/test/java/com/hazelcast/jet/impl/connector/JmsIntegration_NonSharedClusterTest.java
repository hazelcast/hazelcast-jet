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
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.connector.TestJmsBroker.CloseableXAConnectionFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQResource;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jms.ConnectionFactory;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.connector.JmsTestUtil.removeXa;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JmsIntegration_NonSharedClusterTest extends JetTestSupport {

    @ClassRule
    public static EmbeddedActiveMQResource realBroker = new EmbeddedActiveMQResource();
    private static CloseableXAConnectionFactory testBroker = TestJmsBroker.newTestJmsBroker();

    private static volatile boolean storeFailed;

    @AfterClass
    public static void afterClass() throws Exception {
        testBroker.close();
    }

    @Test
    public void when_memberTerminated_then_transactionsRolledBack() throws Exception {
        JetInstance instance1 = createJetMember();
        JetInstance instance2 = createJetMember();

        // use higher number of messages so that each of the parallel processors gets some
        int messageCount = 10_000;
        JmsTestUtil.sendMessages(getConnectionFactorySupplier(false, true).get(), "queue", true, messageCount);

        Pipeline p = Pipeline.create();
        IList<String> sinkList = instance1.getList("sinkList");
        p.readFrom(Sources.jmsQueueBuilder(getConnectionFactorySupplier(true, true))
                          .destinationName("queue")
                          .build(msg -> ((TextMessage) msg).getText()))
         .withoutTimestamps()
         .writeTo(Sinks.list(sinkList));

        instance1.newJob(p, new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(DAYS.toMillis(1)));

        assertTrueEventually(() -> assertEquals("expected items not in sink", messageCount, sinkList.size()));

        // Now forcefully shut down the second member. The terminated member
        // will NOT roll back its transaction. We'll assert that the
        // transactions with processorIndex beyond the current total
        // parallelism are rolled back. We assert that each item is emitted
        // twice, if this was wrong, the items in the non-rolled-back
        // transaction will be stalled and only emitted once, they will be
        // emitted after the default Artemis timeout of 5 minutes.
        instance2.getHazelcastInstance().getLifecycleService().terminate();
        assertTrueEventually(() -> assertEquals("items should be emitted twice", messageCount * 2, sinkList.size()), 20);
    }

    @Test
    public void when_snapshotFails_exactlyOnce_then_jobFails() {
        when_snapshotFails(EXACTLY_ONCE, true);
    }

    @Test
    public void when_snapshotFails_atLeastOnce_then_jobFails() {
        when_snapshotFails(AT_LEAST_ONCE, true);
    }

    @Test
    public void when_snapshotFails_noGuarantee_then_ignored() {
        when_snapshotFails(NONE, false);
    }

    private void when_snapshotFails(ProcessingGuarantee guarantee, boolean expectFailure) {
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
        p.readFrom(Sources.jmsQueue(getConnectionFactorySupplier(true, false), "queue"))
         .withoutTimestamps()
         .writeTo(Sinks.noop());

        Job job = instance.newJob(p, new JobConfig()
                .setProcessingGuarantee(guarantee)
                .setSnapshotIntervalMillis(100));

        if (expectFailure) {
            try {
                job.getFuture().get(20, TimeUnit.SECONDS);
                fail("job did not fail");
            } catch (Exception e) {
                assertContains(e.getMessage(), "the snapshot failed");
            }
            assertTrue(storeFailed);
        } else {
            assertJobStatusEventually(job, RUNNING);
            assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 3);
        }
    }

    private static SupplierEx<ConnectionFactory> getConnectionFactorySupplier(boolean xa, boolean testBroker) {
        return () -> {
            ConnectionFactory cf = testBroker
                    ? (ConnectionFactory) JmsIntegration_NonSharedClusterTest.testBroker
                    : new ActiveMQConnectionFactory(realBroker.getVmURL());
            return xa ? cf : removeXa(cf);
        };
    }

    private static class FailingMapStore extends AMapStore implements Serializable {
        @Override
        public void store(Object o, Object o2) {
            storeFailed = true;
            throw new UnsupportedOperationException("failing map store");
        }
    }
}
