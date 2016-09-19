/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.sink.FileSink;
import com.hazelcast.jet.source.FileSource;
import com.hazelcast.jet.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.strategy.SinglePartitionDistributionStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.JetTestSupport.createVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class DAGTest {

    @Test(expected = IllegalArgumentException.class)
    public void test_add_same_vertex_multiple_times_throws_exception() throws Exception {
        DAG dag = new DAG();
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        dag.addVertex(v1);
        dag.addVertex(v1);
    }

    @Test
    public void test_dag_should_contains_vertex() throws Exception {
        DAG dag = new DAG();
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        dag.addVertex(v1);
        assertTrue("DAG should contain the added vertex", dag.getVertex(v1.getName()) != null);
    }

    @Test
    public void test_empty_dag_should_not_contain_vertex() throws Exception {
        DAG dag = new DAG();
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        assertTrue("DAG should not contain any vertex", dag.getVertex(v1.getName()) == null);
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_empty_dag_throws_exception() throws Exception {
        DAG dag = new DAG();
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_cyclic_graph_throws_exception() throws Exception {
        DAG dag = new DAG();
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addEdge(new Edge("e1", v1, v2));
        dag.addEdge(new Edge("e2", v2, v1));
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_self_cycle_on_vertex_throws_exception() throws Exception {
        DAG dag = new DAG();
        Vertex vertex = createVertex("v1", TestProcessors.Noop.class);
        dag.addVertex(vertex);
        dag.addEdge(new Edge("e1", vertex, vertex));
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_same_output_and_vertex_name_throws_exception() throws Exception {
        DAG dag = new DAG();
        Vertex vertex = createVertex("v1", TestProcessors.Noop.class);
        Vertex output = createVertex("output", TestProcessors.Noop.class);
        vertex.addSink(new FileSink("output"));
        dag.addVertex(vertex);
        dag.addVertex(output);
        dag.addEdge(new Edge("e1", vertex, output));
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_same_input_and_vertex_name_throws_exception() throws Exception {
        DAG dag = new DAG();
        Vertex vertex = createVertex("v1", TestProcessors.Noop.class);
        Vertex input = createVertex("input", TestProcessors.Noop.class);
        vertex.addSource(new FileSource("input"));
        dag.addVertex(vertex);
        dag.addVertex(input);
        dag.addEdge(new Edge("e1", input, vertex));
        dag.validate();
    }


    @Test(expected = IllegalStateException.class)
    public void test_validate_same_source_and_vertex_name_throws_exception() throws Exception {
        DAG dag = new DAG();
        Vertex vertex = createVertex("v1", TestProcessors.Noop.class);
        vertex.addSource(new FileSource("v1"));
        dag.addVertex(vertex);
        dag.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_validate_same_sink_and_vertex_name_throws_exception() throws Exception {
        DAG dag = new DAG();
        Vertex vertex = createVertex("v1", TestProcessors.Noop.class);
        vertex.addSink(new FileSink("v1"));
        dag.addVertex(vertex);
        dag.validate();
    }


    @Test(expected = IllegalStateException.class)
    public void testGetTopologicalVertexIterator_throwsException_whenRemoveIsCalled() throws Exception {
        DAG dag = new DAG();
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        dag.addVertex(v1);
        dag.validate();
        Iterator<Vertex> iterator = dag.getTopologicalVertexIterator();
        iterator.remove();
    }

    @Test(expected = IllegalStateException.class)
    public void testGetTopologicalVertexIterator_throwsException_whenDagIsNotValidated() throws Exception {
        DAG dag = new DAG();
        dag.getTopologicalVertexIterator();
    }


    @Test
    public void testGetTopologicalVertexIterator() throws Exception {
        DAG dag = new DAG();
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        Vertex v3 = createVertex("v3", TestProcessors.Noop.class);

        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addVertex(v3);

        Edge e1 = new Edge("e1", v1, v2);
        Edge e2 = new Edge("e2", v2, v3);

        dag.addEdge(e1);
        dag.addEdge(e2);

        dag.validate();

        ArrayList<Vertex> vertices = new ArrayList<Vertex>();
        vertices.add(v1);
        vertices.add(v2);
        vertices.add(v3);

        ArrayList<Vertex> verticesFromIterator = new ArrayList<Vertex>();
        Iterator<Vertex> iterator = dag.getTopologicalVertexIterator();
        while (iterator.hasNext()) {
            Vertex next = iterator.next();
            verticesFromIterator.add(next);
        }

        assertEquals(vertices, verticesFromIterator);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetRevertedTopologicalVertexIterator_throwsException_whenDagIsNotValidated() throws Exception {
        DAG dag = new DAG();
        dag.getRevertedTopologicalVertexIterator();
    }


    @Test(expected = IllegalStateException.class)
    public void testGetRevertedTopologicalVertexIterator_throwsException_whenRemoveIsCalled() throws Exception {
        DAG dag = new DAG();
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        dag.addVertex(v1);
        dag.validate();
        Iterator<Vertex> iterator = dag.getRevertedTopologicalVertexIterator();
        iterator.remove();
    }

    @Test
    public void testGetRevertedTopologicalVertexIterator() throws Exception {
        DAG dag = new DAG();
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        Vertex v3 = createVertex("v3", TestProcessors.Noop.class);

        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addVertex(v3);

        Edge e1 = new Edge("e1", v1, v2);
        Edge e2 = new Edge("e2", v2, v3);

        dag.addEdge(e1);
        dag.addEdge(e2);

        dag.validate();

        ArrayList<Vertex> vertices = new ArrayList<Vertex>();
        vertices.add(v3);
        vertices.add(v2);
        vertices.add(v1);

        ArrayList<Vertex> verticesFromIterator = new ArrayList<Vertex>();
        Iterator<Vertex> iterator = dag.getRevertedTopologicalVertexIterator();
        while (iterator.hasNext()) {
            Vertex next = iterator.next();
            verticesFromIterator.add(next);
        }

        assertEquals(vertices, verticesFromIterator);
    }

    @Test
    public void testDAG_Serialization_Deserialization() throws Exception {
        DAG dag = new DAG("dag");
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        Vertex v3 = createVertex("v3", TestProcessors.Noop.class);

        Edge e1 = new Edge("e1", v1, v2)
                .partitioned(StringAndPartitionAwarePartitioningStrategy.INSTANCE, SerializedHashingStrategy.INSTANCE)
                .distributed(new SinglePartitionDistributionStrategy("e1"))
                .broadcast();
        Edge e2 = new Edge("e2", v2, v3)
                .partitioned(StringPartitioningStrategy.INSTANCE, SerializedHashingStrategy.INSTANCE)
                .distributed(new SinglePartitionDistributionStrategy("e2"));

        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addVertex(v3);

        dag.addEdge(e1);
        dag.addEdge(e2);

        DefaultSerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
        SerializationService serializationService = builder.build();
        Data data = serializationService.toData(dag);
        DAG deSerializedDag = serializationService.toObject(data);

        assertEquals("dag", deSerializedDag.getName());
        assertEquals(v1, deSerializedDag.getVertex("v1"));
        assertEquals(v2, deSerializedDag.getVertex("v2"));
        assertEquals(v3, deSerializedDag.getVertex("v3"));

    }

    @Test
    public void testDAG_GetVertices() throws Exception {
        DAG dag = new DAG("dag");
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        Vertex v3 = createVertex("v3", TestProcessors.Noop.class);

        dag.addVertex(v1);
        dag.addVertex(v2);
        dag.addVertex(v3);

        Collection<Vertex> vertices = dag.getVertices();
        assertEquals(3, vertices.size());
        assertTrue(vertices.contains(v1));
        assertTrue(vertices.contains(v2));
        assertTrue(vertices.contains(v3));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testDAG_addEdgeMultipleTimes_throwsException() throws Exception {
        DAG dag = new DAG("dag");
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        Edge e1 = new Edge("e1", v1, v2);
        dag.addVertex(v1);
        dag.addVertex(v2);

        dag.addEdge(e1);
        dag.addEdge(e1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDAG_addEdge_withoutCorrespondingInput_throwsException() throws Exception {
        DAG dag = new DAG("dag");
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        Edge e1 = new Edge("e1", v1, v2);
        dag.addVertex(v2);

        dag.addEdge(e1);
        dag.addEdge(e1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDAG_addEdge_withoutCorrespondingOutput_throwsException() throws Exception {
        DAG dag = new DAG("dag");
        Vertex v1 = createVertex("v1", TestProcessors.Noop.class);
        Vertex v2 = createVertex("v2", TestProcessors.Noop.class);
        Edge e1 = new Edge("e1", v1, v2);
        dag.addVertex(v1);
        dag.addEdge(e1);
    }

}
