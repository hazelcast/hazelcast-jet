/*
 * Original work Copyright (c) 2016 The Apache Software Foundation
 * Modified work Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.stream.DistributedCollectors.toList;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * Computes a topological ordering of the vertices in a graph.
 * Validates against cycles.
 */
public final class TopologicalSorter<V> {
    // Consulted, but not updated, by the algorithm:
    private final Map<TarjanVertex<V>, List<TarjanVertex<V>>> adjacencyMap;
    private final Function<V, String> vertexNameF;

    // Updated by the algorithm:
    private final ArrayDeque<V> topologicallySorted = new ArrayDeque<>();
    private final Deque<TarjanVertex<V>> tarjanStack = new ArrayDeque<>();
    private int nextIndex;

    private TopologicalSorter(
            Map<TarjanVertex<V>, List<TarjanVertex<V>>> adjacencyMap,
            Function<V, String> vertexNameF
    ) {
        this.adjacencyMap = adjacencyMap;
        this.vertexNameF = vertexNameF;
    }

    /**
     * Returns an iterable that will encounter the vertices of a graph in
     * a topological order (the order is not unique). If the graph cannot
     * be topologically ordered due to the presence of a cycle, it will
     * throw an exception.
     *
     * @param adjacencyMap the description of the graph: for each vertex,
     *                     a list of its adjacent vertices
     * @param vertexNameF a function that returns a vertex's name, used to generate
     *                    diagnostic information in the case of a cycle in the graph
     * @param <V> type used to represent the vertices
     */
    public static <V> Iterable<V> topologicalSort(
            Map<V, List<V>> adjacencyMap, Function<V, String> vertexNameF
    ) {
        // fill in missing map entries
        adjacencyMap.values().stream()
                    .flatMap(List::stream)
                    .collect(toList())
                    .forEach(v -> adjacencyMap.putIfAbsent(v, emptyList()));
        Map<V, TarjanVertex<V>> tarjanVertices =
                adjacencyMap.keySet().stream()
                            .map(v -> entry(v, new TarjanVertex<>(v)))
                            .collect(toMap(Entry::getKey, Entry::getValue));
        Map<TarjanVertex<V>, List<TarjanVertex<V>>> tarjanAdjacencyMap =
                adjacencyMap.entrySet().stream()
                            .collect(toMap(e -> tarjanVertices.get(e.getKey()),
                                           e -> e.getValue().stream()
                                                 .map(tarjanVertices::get)
                                                 .collect(toList())));
        return new TopologicalSorter<>(tarjanAdjacencyMap, vertexNameF).go();
    }

    // Partial implementation of Tarjan's algorithm:
    // http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    // https://rjlipton.files.wordpress.com/2009/10/dfs1971.pdf
    //
    // The full algorithm outputs all strongly-connected (SC) components of a
    // graph; this code just finds any SC component involving more than a
    // single vertex.
    private Iterable<V> go() {
        for (TarjanVertex<V> tv : adjacencyMap.keySet()) {
            if (tv.index != -1) {
                continue;
            }
            // The stack invariant:
            // Vertices are placed on a stack in the order in which they are visited.
            // When the depth-first search recursively visits a vertex v and its
            // descendants, those vertices are not all necessarily popped from the
            // stack when this recursive call returns. The invariant is that a vertex
            // remains on the stack after it has been visited if and only if there is
            // a path from it to some vertex earlier on the stack.
            assert tarjanStack.isEmpty() : "Broken stack invariant";
            strongconnect(tv);
        }
        return topologicallySorted;
    }

    // method name identical to the one used in the Wikipedia article
    private void strongconnect(TarjanVertex<V> currTv) {
        currTv.visitedAtIndex(nextIndex++);
        push(currTv);
        for (TarjanVertex<V> outTv : adjacencyMap.get(currTv)) {
            if (outTv == currTv) {
                throw new IllegalArgumentException(
                        "Vertex " + vertexNameF.apply(currTv.v) + " is connected to itself");
            }
            if (outTv.index == -1) {
                // outTv not discovered yet, visit it...
                strongconnect(outTv);
                // ... and propagate lowlink computed for it to currTv
                currTv.lowlink = min(currTv.lowlink, outTv.lowlink);
            } else if (outTv.onstack) {
                // outTv is already on the stack => there is a cycle in the graph.
                // Proceed with the algorithm until the full extent of the cycle
                // is known.
                currTv.lowlink = min(currTv.lowlink, outTv.index);
            }
        }
        if (currTv.lowlink < currTv.index) {
            // currTv has a path to some vertex that is already on the stack.
            // Leave currTv on the stack and return.
            return;
        }
        assert currTv.lowlink == currTv.index : "Broken lowlink invariant";
        // currTv is the root of an SC component. Find out if the component has
        // more than one member.
        TarjanVertex<V> popped = pop();
        if (popped == currTv) {
            // currTv was on the top of the stack => it is the sole member of its SC
            // component => it is not involved in any cycles. Add it to the output
            // list and return.
            topologicallySorted.addFirst(currTv.v);
            return;
        }
        // There are vertices on the stack beyond currTv => it is not the sole
        // member of its SC component => it is involved in a cycle. Report an
        // error with a list of all the members of the SC component.
        //
        // At this point the algorithm is over. The following stack operations
        // are not a part of it, their sole purpose is generating the desired
        // error message.
        while (tarjanStack.peekFirst() != currTv) {
            tarjanStack.removeFirst();
        }
        tarjanStack.addLast(popped);
        tarjanStack.addLast(currTv);
        throw new IllegalArgumentException("DAG contains a cycle: "
                + tarjanStack.stream()
                             .map(av -> vertexNameF.apply(av.v))
                             .collect(joining(" -> ")));
    }

    private void push(TarjanVertex<V> thisTv) {
        thisTv.onstack = true;
        tarjanStack.addLast(thisTv);
    }

    private TarjanVertex<V> pop() {
        TarjanVertex<V> popped = tarjanStack.removeLast();
        popped.onstack = false;
        return popped;
    }

    private static final class TarjanVertex<V> {
        V v;

        // Field names identical to those used in the Wikipedia article:
        int index = -1;
        int lowlink = -1;
        boolean onstack; // tells whether the vertex is currently on the Tarjan stack

        TarjanVertex(V v) {
            this.v = v;
        }

        void visitedAtIndex(int index) {
            this.index = index;
            this.lowlink = index;
        }

        @Override
        public String toString() {
            return v.toString();
        }
    }
}
