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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.Arrays.asList;

@RunWith(HazelcastSerialClassRunner.class)
public class LightJobTest extends JetTestSupport {

    private JetInstance inst;

    @Before
    public void before() {
        inst = createJetMember();
    }

    @Test
    public void test_member() {
        test(inst);
    }

    @Test
    public void test_client() {
        test(createJetClient());
    }

    private void test(JetInstance submittingInstance) {
        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", ListSource.supplier(asList(1, 2, 3)));
        Vertex sink = dag.newVertex("sink", DiagnosticProcessors.writeLoggerP());
        dag.edge(between(src, sink).distributed());

        submittingInstance.newLightJob(dag).join();
    }
}
