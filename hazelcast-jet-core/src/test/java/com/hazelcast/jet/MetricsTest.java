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

package com.hazelcast.jet;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.MetricsPlugin;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.util.CompressingProbeRenderer;
import org.junit.Test;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.impl.util.CompressingProbeRenderer.decompress;

public class MetricsTest extends JetTestSupport {

    @Test
    public void test() throws InterruptedException {
        System.setProperty(Diagnostics.ENABLED.getName(), "true");
        System.setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.INFO.name());
        System.setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");

        JetInstance inst = createJetMember();
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", Generator::new).localParallelism(1);
        Vertex sink = dag.newVertex("sink", noopP()).localParallelism(1);
        dag.edge(between(source, sink));
        inst.newJob(dag);

        MetricsRegistry mr = ((HazelcastInstanceImpl) inst.getHazelcastInstance()).node.nodeEngine.getMetricsRegistry();
        CompressingProbeRenderer renderer = new CompressingProbeRenderer(1024);
        mr.render(renderer);
        decompress(renderer.getRenderedBlob(), new ProbeRenderer() {
            @Override
            public void renderLong(String name, long value) {
                System.out.println("long, " + name + ": " + value);
            }

            @Override
            public void renderDouble(String name, double value) {
                System.out.println("long, " + name + ": " + value);
            }

            @Override
            public void renderException(String name, Exception e) {

            }

            @Override
            public void renderNoValue(String name) {

            }
        });

        Thread.sleep(30000);
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
