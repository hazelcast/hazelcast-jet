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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(HazelcastSerialClassRunner.class)
public class ParallelJobSubmissionTest extends JetTestSupport {
    @Test
    public void test() throws Exception {
        /*
        This test tests the issue #1339 (https://github.com/hazelcast/hazelcast-jet/issues/1339)
        There's no assert in this test. If the problem reproduces, the jobs won't complete and will
        get stuck.
         */
        DAG dag = new DAG();
        dag.newVertex("p", TestProcessors.ListSource.supplier(Arrays.asList(1, 2, 3)));

        JetInstance instance = createJetMember();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<Job>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(executor.submit(() -> instance.newJob(dag)));
        }
        for (Future<Job> future : futures) {
            future.get().join();
        }
    }
}
