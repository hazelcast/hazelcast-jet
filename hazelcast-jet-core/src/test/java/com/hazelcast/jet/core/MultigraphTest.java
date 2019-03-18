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
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.processor.SinkProcessors;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekInputP;
import static java.util.function.Function.identity;
import static org.junit.Assert.assertEquals;

public class MultigraphTest extends JetTestSupport {

    @Test
    public void test() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", ListSource.supplier(Arrays.asList(1, 2, 3)));
        Vertex sink = dag.newVertex("sink", peekInputP(SinkProcessors.writeListP("sink")));
        dag.edge(Edge.from(source, 0).to(sink, 0));
        dag.edge(Edge.from(source, 1).to(sink, 1));

        JetInstance instance = createJetMember();
        instance.newJob(dag).join();

        assertEquals(new HashMap<Integer, Long>() {{
            put(1, 2L);
            put(2, 2L);
            put(3, 2L);
        }}, instance.getList("sink").stream().collect(Collectors.groupingBy(identity(), Collectors.counting())));
    }
}
