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

package com.hazelcast.jet;

import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.Repeat;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class LeakTest extends JetTestSupport {

    @Test
    @Repeat(100)
    public void test() throws InterruptedException {
        JetConfig config = new JetConfig();
        config.getMetricsConfig().setMetricsForDataStructuresEnabled(true);
        config.getMetricsConfig().setCollectionIntervalSeconds(1);
        JetInstance jet = createJetMember(config);
        Pipeline p = Pipeline.create();

        jet.getMap("source").put(1, 1);

        p.drawFrom(Sources.map("source"))
                .drainTo(Sinks.map("sink"));

        List<Job> jobs = new ArrayList<>();
        JobConfig jobConfig = new JobConfig();
        jobConfig.addResource("/Users/can/src/temp/analytics/src/main/java/com/sigmastream/analytics/JobTest.java");
        for (int i = 0; i < 1000; i++) {
            jobs.add(jet.newJob(p, jobConfig));
        }
        for (Job job : jobs) {
            job.join();
        }

        assertTrueAllTheTime(() -> {
            System.out.println(jet.getHazelcastInstance()
                    .getDistributedObjects());
            assertEquals(emptyList(), getResourceMaps(jet));
        }, 10);
    }

    private List<String> getResourceMaps(JetInstance jet) {
        return jet.getHazelcastInstance()
                .getDistributedObjects()
                .stream()
                .map(e -> e.getName())
                .filter(e -> e.startsWith(JobRepository.RESOURCES_MAP_NAME_PREFIX))
                .collect(Collectors.toList());
    }
}
