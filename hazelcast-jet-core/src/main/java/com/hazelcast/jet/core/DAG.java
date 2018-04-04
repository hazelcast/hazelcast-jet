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

import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.SerializationConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.TopologicalSorter.topologicalSort;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.emptyList;
import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.groupingBy;

/**
 * Describes a computation to be performed by the Jet computation engine.
 * A {@link Vertex vertex} represents a unit of data processing and an
 * {@link Edge edge} represents the path along which the data travels to
 * the next vertex.
 * <p>
 * The work of a single vertex is parallelized and distributed, so that there are
 * several instances of the {@link Processor} type on each member
 * corresponding to it. Whenever possible, each instance should be
 * tasked with only a slice of the total data and a partitioning strategy
 * can be employed to ensure that the data sent to each vertex is collated
 * by a partitioning key.
 * <p>
 * There are three basic kinds of vertices:
 * <ol><li>
 *     <em>source</em> with just outbound edges;
 * </li><li>
 *     <em>processor</em> with both inbound and outbound edges;
 * </li><li>
 *     <em>sink</em> with just inbound edges.
 * </li></ol>
 * Data travels from sources to sinks and is transformed and reshaped
 * as it passes through the processors.
 */
public class DAG implements IdentifiedDataSerializable, Iterable<Vertex> {

    private Set<Edge> edges = new LinkedHashSet<>();
    private Map<String, Vertex> nameToVertex = new HashMap<>();
    // Transient field:
    private Set<Vertex> verticesByIdentity = newSetFromMap(new IdentityHashMap<Vertex, Boolean>());

    /**
     * Creates a vertex from a {@code Supplier<Processor>} and adds it to this DAG.
     *
     * @see Vertex#Vertex(String, DistributedSupplier)
     *
     * @param name the unique name of the vertex
     * @param simpleSupplier the simple, parameterless supplier of {@code Processor} instances
     */
    public Vertex newVertex(String name, DistributedSupplier<? extends Processor> simpleSupplier) {
        return addVertex(new Vertex(name, simpleSupplier));
    }

    /**
     * Creates a vertex from a {@code ProcessorSupplier} and adds it to this DAG.
     *
     * @see Vertex#Vertex(String, ProcessorSupplier)
     *
     * @param name the unique name of the vertex
     * @param processorSupplier the supplier of {@code Processor} instances which will be used on all members
     */
    public Vertex newVertex(String name, ProcessorSupplier processorSupplier) {
        return addVertex(new Vertex(name, processorSupplier));
    }

    /**
     * Creates a vertex from a {@code ProcessorMetaSupplier} and adds it to this DAG.
     *
     * @see Vertex#Vertex(String, ProcessorMetaSupplier)
     *
     * @param name the unique name of the vertex
     * @param metaSupplier the meta-supplier of {@code ProcessorSupplier}s for each member
     *
     */
    public Vertex newVertex(String name, ProcessorMetaSupplier metaSupplier) {
        return addVertex(new Vertex(name, metaSupplier));
    }

    /**
     * Adds a vertex to this DAG. The vertex name must be unique.
     */
    public DAG vertex(Vertex vertex) {
        addVertex(vertex);
        return this;
    }

    /**
     * Adds an edge to this DAG. The vertices it connects must already be present
     * in the DAG. It is an error to add an edge that connects the same two
     * vertices as another existing edge. It is an error to connect an edge to
     * a vertex at the same ordinal as another existing edge. However, inbound
     * and outbound ordinals are independent, so there can be two edges at the
     * same ordinal, one inbound and one outbound.
     */
    public DAG edge(Edge edge) {
        if (edge.getDestination() == null) {
            throw new IllegalArgumentException("Edge has no destination");
        }
        if (edges.contains(edge)) {
            throw new IllegalArgumentException("This DAG already has an edge between '" + edge.getSourceName()
                    + "' and '" + edge.getDestName() + '\'');
        }
        if (!containsVertex(edge.getSource())) {
            throw new IllegalArgumentException(
                    containsVertexName(edge.getSource())
                            ? "This DAG has a vertex called '" + edge.getSourceName()
                            + "', but the supplied edge's source is a different vertex with the same name"
                            : "Source vertex '" + edge.getSourceName() + "' is not in this DAG"
            );
        }
        if (!containsVertex(edge.getDestination())) {
            throw new IllegalArgumentException(
                    containsVertexName(edge.getDestination())
                            ? "This DAG has a vertex called '" + edge.getDestName()
                            + "', but the supplied edge's destination is a different vertex with the same name"
                            : "Destination vertex '" + edge.getDestName() + "' is not in this DAG");
        }
        if (getInboundEdges(edge.getDestName())
                .stream().anyMatch(e -> e.getDestOrdinal() == edge.getDestOrdinal())) {
            throw new IllegalArgumentException("Vertex '" + edge.getDestName()
                    + "' already has an inbound edge at ordinal " + edge.getDestOrdinal()
                    + (edge.getSourceOrdinal() == 0 && edge.getDestOrdinal() == 0
                    ? ", use Edge.from().to() to specify another ordinal" : ""));
        }
        if (getOutboundEdges(edge.getSourceName())
                .stream().anyMatch(e -> e.getSourceOrdinal() == edge.getSourceOrdinal())) {
            throw new IllegalArgumentException("Vertex '" + edge.getSourceName()
                    + "' already has an outbound edge at ordinal " + edge.getSourceOrdinal()
                    + (edge.getSourceOrdinal() == 0 && edge.getDestOrdinal() == 0
                    ? ", use Edge.from().to() to specify another ordinal" : ""));
        }
        if (edge.getSource() == edge.getDestination()) {
            throw new IllegalArgumentException("Attempted to add an edge from " + edge.getSourceName() + " to itself");
        }
        edges.add(edge);
        return this;
    }

    /**
     * Returns the inbound edges connected to the vertex with the given name.
     */
    public List<Edge> getInboundEdges(String vertexName) {
        if (!nameToVertex.containsKey(vertexName)) {
            throw new IllegalArgumentException("No vertex with name '" + vertexName + "' found in this DAG");
        }
        List<Edge> inboundEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getDestName().equals(vertexName)) {
                inboundEdges.add(edge);
            }
        }
        return inboundEdges;
    }

    /**
     * Returns the outbound edges connected to the vertex with the given name.
     */
    public List<Edge> getOutboundEdges(String vertexName) {
        if (!nameToVertex.containsKey(vertexName)) {
            throw new IllegalArgumentException("No vertex with name '" + vertexName + "' found in this DAG");
        }
        List<Edge> outboundEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getSourceName().equals(vertexName)) {
                outboundEdges.add(edge);
            }
        }
        return outboundEdges;
    }

    /**
     * Returns the vertex with the given name.
     */
    public Vertex getVertex(String vertexName) {
        return nameToVertex.get(vertexName);
    }

    /**
     * Returns an iterator over the DAG's vertices in topological order.
     */
    @Override
    public Iterator<Vertex> iterator() {
        return validate().iterator();
    }

    private Vertex addVertex(Vertex vertex) {
        if (nameToVertex.containsKey(vertex.getName())) {
            throw new IllegalArgumentException("Vertex " + vertex.getName() + " is already defined.");
        }
        verticesByIdentity.add(vertex);
        nameToVertex.put(vertex.getName(), vertex);
        return vertex;
    }

    private boolean containsVertex(Vertex vertex) {
        return verticesByIdentity.contains(vertex);
    }

    private boolean containsVertexName(Vertex vertex) {
        return nameToVertex.containsKey(vertex.getName());
    }

    /**
     * @return iterable over vertices in topological order
     */
    // exposed for testing
    Iterable<Vertex> validate() {
        checkTrue(!nameToVertex.isEmpty(), "DAG must contain at least one vertex");
        Map<String, List<Edge>> inboundEdgeMap = edges.stream().collect(groupingBy(Edge::getDestName));
        Map<String, List<Edge>> outboundEdgeMap = edges.stream().collect(groupingBy(Edge::getSourceName));
        validateInboundEdgeOrdinals(inboundEdgeMap);
        validateOutboundEdgeOrdinals(outboundEdgeMap);

        Map<Vertex, List<Vertex>> adjacencyMap = new HashMap<>();
        for (Edge edge : edges) {
            adjacencyMap.computeIfAbsent(edge.getSource(), x -> new ArrayList<>())
                        .add(edge.getDestination());
        }
        for (Vertex v : nameToVertex.values()) {
            adjacencyMap.computeIfAbsent(v, x -> emptyList());
        }
        return topologicalSort(adjacencyMap, Vertex::getName);
    }

    private static void validateInboundEdgeOrdinals(Map<String, List<Edge>> inboundEdgeMap) {
        for (Map.Entry<String, List<Edge>> entry : inboundEdgeMap.entrySet()) {
            String vertex = entry.getKey();
            int[] ordinals = entry.getValue().stream().mapToInt(Edge::getDestOrdinal).sorted().toArray();
            for (int i = 0; i < ordinals.length; i++) {
                if (ordinals[i] != i) {
                    throw new IllegalArgumentException("Input ordinals for vertex " + vertex
                            + " are not properly numbered. Actual: " + Arrays.toString(ordinals)
                            + " Expected: " + Arrays.toString(IntStream.range(0, ordinals.length).toArray()));
                }
            }
        }
    }

    private static void validateOutboundEdgeOrdinals(Map<String, List<Edge>> outboundEdgeMap) {
        for (Map.Entry<String, List<Edge>> entry : outboundEdgeMap.entrySet()) {
            String vertex = entry.getKey();
            int[] ordinals = entry.getValue().stream().mapToInt(Edge::getSourceOrdinal).sorted().toArray();
            for (int i = 0; i < ordinals.length; i++) {
                if (ordinals[i] != i) {
                    throw new IllegalArgumentException("Output ordinals for vertex " + vertex + " are not ordered. "
                            + "Actual: " + Arrays.toString(ordinals) + " Expected: "
                            + Arrays.toString(IntStream.range(0, ordinals.length).toArray()));
                }
            }
        }
    }

    @Override
    public String toString() {
        return toString(-1);
    }

    /**
     * Returns a string representation of the DAG.
     *
     * @param defaultLocalParallelism the local parallelism that will be shown if
     *                                neither overridden on the vertex nor the
     *                                preferred parallelism is defined by
     *                                meta-supplier
     */
    public String toString(int defaultLocalParallelism) {
        final StringBuilder b = new StringBuilder("dag\n");
        for (Vertex v : this) {
            b.append("    .vertex(\"").append(v.getName()).append("\")");
            int localParallelism = v.getLocalParallelism();
            if (localParallelism == -1) {
                localParallelism = v.getMetaSupplier().preferredLocalParallelism();
                if (localParallelism == -1) {
                    localParallelism = defaultLocalParallelism;
                }
            }
            if (localParallelism != -1) {
                b.append(".localParallelism(").append(localParallelism).append(')');
            }
            b.append('\n');
        }
        for (Edge e : edges) {
            b.append("    .edge(").append(e).append(")\n");
        }
        return b.toString();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(nameToVertex.size());

        for (Map.Entry<String, Vertex> entry : nameToVertex.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }

        out.writeInt(edges.size());

        for (Edge edge : edges) {
            out.writeObject(edge);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int vertexCount = in.readInt();

        for (int i = 0; i < vertexCount; i++) {
            String key = in.readObject();
            Vertex value = in.readObject();
            nameToVertex.put(key, value);
        }

        int edgeCount = in.readInt();

        for (int i = 0; i < edgeCount; i++) {
            Edge edge = in.readObject();
            edge.restoreSourceAndDest(nameToVertex);
            edges.add(edge);
        }

        verticesByIdentity.addAll(nameToVertex.values());
    }

    @Override
    public int getFactoryId() {
        return SerializationConstants.FACTORY_ID;
    }

    @Override
    public int getId() {
        return SerializationConstants.DAG;
    }
}
