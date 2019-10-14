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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.managementcenter.Metric;
import com.hazelcast.internal.metrics.managementcenter.MetricConsumer;
import com.hazelcast.internal.metrics.managementcenter.MetricsCompressor;
import com.hazelcast.internal.metrics.managementcenter.MetricsResultSet;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.version.MemberVersion;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class ReadMetricsTest extends JetTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_readMetricsAsync() throws Exception {
        JetConfig conf = new JetConfig();
        conf.getMetricsConfig().setCollectionIntervalSeconds(1);
        conf.getMetricsConfig().setMinimumLevel(ProbeLevel.INFO);
        JetInstance instance = createJetMember(conf);
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();
        Member member = instance.getHazelcastInstance().getCluster().getLocalMember();

        long nextSequence = 0;
        for (int i = 0; i < 3; i++) {
            MetricsResultSet result = client.readMetricsAsync(member, nextSequence).get();
            nextSequence = result.nextSequence();
            // call should not return empty result - it should wait until a result is available
            assertFalse("empty result", result.collections().isEmpty());
            final MetricKeyConsumer metricConsumer = new MetricKeyConsumer();
            assertTrue(metricStream(result.collections().get(0).getValue())
                    .map(metric -> {
                        metric.provide(metricConsumer);
                        return metricConsumer.key;
                    })
                    .anyMatch(name -> name.equals("[unit=count,metric=cluster.size]")));
        }
    }

    @Test
    public void when_invalidUUID() throws ExecutionException, InterruptedException {
        JetInstance instance = createJetMember();
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();
        Address addr = instance.getCluster().getLocalMember().getAddress();
        MemberVersion ver = instance.getCluster().getLocalMember().getVersion();
        MemberImpl member = new MemberImpl(addr, ver, false, UuidUtil.newUnsecureUUID());

        exception.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
        client.readMetricsAsync(member, 0).get();
    }

    @Test
    public void when_metricsDisabled() throws ExecutionException, InterruptedException {
        JetConfig cfg = new JetConfig();
        cfg.getMetricsConfig().setEnabled(false);
        JetInstance instance = createJetMember(cfg);
        JetClientInstanceImpl client = (JetClientInstanceImpl) createJetClient();

        exception.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
        client.readMetricsAsync(instance.getCluster().getLocalMember(), 0).get();
    }

    private static Stream<Metric> metricStream(byte[] bytes) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        MetricsCompressor.decompressingIterator(bytes),
                        Spliterator.NONNULL
                ), false
        );
    }

    private static class MetricKeyConsumer implements MetricConsumer {
        String key;

        @Override
        public void consumeLong(String key, long value) {
            this.key = key;
        }

        @Override
        public void consumeDouble(String key, double value) {
            this.key = key;
        }
    }
}
