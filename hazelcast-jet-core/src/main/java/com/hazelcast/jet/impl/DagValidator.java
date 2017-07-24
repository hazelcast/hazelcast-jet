/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Vertex;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * Validates a DAG against cycles and other malformations.
 */
public class DagValidator {
    // Consulted, but not updated, by the cycle detecting algorithm:
    private final Map<String, List<Edge>> outboundEdgeMap;
    private final Map<String, AnnotatedVertex> avMap;

    // Updated by the cycle detecting algorithm:
    private final List<Vertex> reverseTopoOrder = new ArrayList<>();
    private final Deque<AnnotatedVertex> avStack = new ArrayDeque<>();
    private int nextIndex;

    private DagValidator(Map<String, AnnotatedVertex> annotatedVertexMap, Map<String, List<Edge>> outboundEdgeMap) {
        this.outboundEdgeMap = unmodifiableMap(outboundEdgeMap);
        this.avMap = unmodifiableMap(annotatedVertexMap);
    }

    /**
     * @return vertices in reverse topological order
     */
    public static List<Vertex> validate(Map<String, Vertex> verticesByName, Set<Edge> edges) {
        checkTrue(!verticesByName.isEmpty(), "DAG must contain at least one vertex");
        validateInboundEdgeOrdinals(edges.stream().collect(groupingBy(Edge::getDestName)));

        Map<String, AnnotatedVertex> annotatedVertexMap = verticesByName
                .entrySet().stream()
                .collect(toMap(Entry::getKey, v -> new AnnotatedVertex(v.getValue())));
        Map<String, List<Edge>> outboundEdgeMap = edges.stream().collect(groupingBy(Edge::getSourceName));
        return new DagValidator(annotatedVertexMap, outboundEdgeMap).validate();
    }

    private List<Vertex> validate() {
        validateOutboundEdgeOrdinals();
        detectCycles();
        return reverseTopoOrder;
    }

    private static void validateInboundEdgeOrdinals(Map<String, List<Edge>> incomingEdgeMap) {
        for (Map.Entry<String, List<Edge>> entry : incomingEdgeMap.entrySet()) {
            String vertex = entry.getKey();
            int[] ordinals = entry.getValue().stream().mapToInt(Edge::getDestOrdinal).sorted().toArray();
            for (int i = 0; i < ordinals.length; i++) {
                if (ordinals[i] != i) {
                    throw new IllegalArgumentException("Input ordinals for vertex " + vertex + " are not ordered. "
                            + "Actual: " + Arrays.toString(ordinals) + " Expected: "
                            + Arrays.toString(IntStream.range(0, ordinals.length).toArray()));
                }
            }
        }
    }

    private void validateOutboundEdgeOrdinals() {
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

    // Part of Tarjan's algorithm that identifies strongly connected components
    // of a directed graph.
    // http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    private void detectCycles() {
        for (AnnotatedVertex av : avMap.values()) {
            if (av.index != -1) {
                continue;
            }
            assert avStack.isEmpty();
            strongConnect(av);
        }
    }

    private void strongConnect(AnnotatedVertex thisAv) {
        thisAv.index = nextIndex;
        thisAv.lowlink = nextIndex;
        nextIndex++;
        avStack.addLast(thisAv);
        thisAv.isOnStack = true;

        for (Edge outEdge : outEdges(thisAv)) {
            AnnotatedVertex outAv = avMap.get(outEdge.getDestName());
            if (outAv.index == -1) {
                strongConnect(outAv);
                thisAv.lowlink = Math.min(thisAv.lowlink, outAv.lowlink);
            } else if (outAv.isOnStack) {
                // This already means there is a cycle, but we'll proceed with the
                // algorithm until the full cycle can be displayed.
                thisAv.lowlink = Math.min(thisAv.lowlink, outAv.index);
            }
        }
        if (thisAv.lowlink == thisAv.index) {
            AnnotatedVertex popped = avStack.removeLast();
            popped.isOnStack = false;
            if (popped == thisAv) {
                // No cycles detected involving thisAv. Add it to the output vertex list.
                reverseTopoOrder.add(thisAv.v);
            } else {
                // A vertex other than thisAv was left on the stack. This indicates there is
                // a cycle. It consists of all the nodes from the top of the stack to thisAv.
                throw new IllegalArgumentException("DAG contains a cycle: " + cycleToString(popped));
            }
        }
    }

    private List<Edge> outEdges(AnnotatedVertex thisAv) {
        return outboundEdgeMap.getOrDefault(thisAv.v.getName(), emptyList());
    }

    // Destructive operation, corrupts avStack
    private String cycleToString(AnnotatedVertex popped) {
        avStack.addLast(popped);
        avStack.addLast(avStack.getFirst());
        return avStack.stream().map(av -> av.v.getName()).collect(joining(" -> "));
    }

    private static final class AnnotatedVertex {
        Vertex v;
        int index;
        int lowlink;
        boolean isOnStack;

        private AnnotatedVertex(Vertex v) {
            this.v = v;
            index = -1;
            lowlink = -1;
        }
    }
}
