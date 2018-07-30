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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckForeverSourceP;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class UpscalingTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;
    private static final long UPSCALE_DELAY = SECONDS.toMillis(1);

    private JetInstance[] instances;
    private DAG dag;

    @Before
    public void setup() {
        TestProcessors.reset(NODE_COUNT * LOCAL_PARALLELISM);

        dag = new DAG().vertex(new Vertex("test", new MockPS(StuckForeverSourceP::new, NODE_COUNT)));
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setUpscaleDelayMillis(UPSCALE_DELAY);
        instances = createJetMembers(config, NODE_COUNT);
    }

    @Test
    public void when_memberAdded_then_jobUpscaled() {
        Job job = instances[0].newJob(dag);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()), 10);

        createJetMember();
        assertTrueEventually(() -> assertEquals(NODE_COUNT * 2 + 1, MockPS.initCount.get()), 10);
    }
}
