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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.junit.Test;

public class MetricsTest extends JetTestSupport {

    @Test
    public void test() throws InterruptedException {
        JetInstance inst = createJetMember();
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map("source"))
                .drainTo(Sinks.map("sink"));
        inst.newJob(p, new JobConfig().setName("my first job ")).join();


        while (true) {
            MetricsRegistry mr =
                    ((HazelcastInstanceImpl) inst.getHazelcastInstance()).node.nodeEngine.getMetricsRegistry();
            CompressingProbeRenderer renderer = new CompressingProbeRenderer(1024);
            mr.render(renderer);
            MetricsResultSet.MetricsCollection collection =
                    new MetricsResultSet.MetricsCollection(0, renderer.getRenderedBlob());

            for (Metric metric : collection) {
                System.out.println(metric);
            }
            Thread.sleep(1000);
        }

//        Thread.sleep(30000);
    }

    public static class Generator extends AbstractProcessor {
        private int seq;

        @Override
        public boolean complete() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            tryEmit(seq++);
            return false;
        }
    }

}
