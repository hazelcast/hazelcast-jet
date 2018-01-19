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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.pipeline.transform.CoGroupTransform;
import com.hazelcast.jet.impl.pipeline.transform.FilterTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapTransform;
import com.hazelcast.jet.impl.pipeline.transform.GroupTransform;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapTransform;
import com.hazelcast.jet.impl.pipeline.transform.PeekTransform;
import com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform;
import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.impl.processor.HashJoinCollectP;
import com.hazelcast.jet.impl.processor.HashJoinP;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.SessionWindowDef;
import com.hazelcast.jet.pipeline.SlidingWindowDef;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekOutputP;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.accumulateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSessionWindowP;
import static com.hazelcast.jet.core.processor.Processors.coAccumulateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.combineByKeyP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.filterP;
import static com.hazelcast.jet.core.processor.Processors.flatMapP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.TopologicalSorter.topologicalSort;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("unchecked")
class Planner {

    private final PipelineImpl pipeline;
    private final DAG dag = new DAG();
    private final Map<Transform, PlannerVertex> xform2vertex = new HashMap<>();

    private final Set<String> vertexNames = new HashSet<>();

    Planner(PipelineImpl pipeline) {
        this.pipeline = pipeline;
    }

    DAG createDag() {
        Map<Transform, List<Transform>> adjacencyMap = pipeline.adjacencyMap();
        validateNoLeakage(adjacencyMap);
        Iterable<Transform> sorted = topologicalSort(adjacencyMap, Object::toString);
        for (Transform transform : sorted) {
            propagateEmitsJetEvents(transform, adjacencyMap);
            if (transform instanceof BatchSourceTransform) {
                handleSource((BatchSourceTransform) transform);
            } else if (transform instanceof StreamSourceTransform) {
                handleStreamSource((StreamSourceTransform) transform);
            } else if (transform instanceof ProcessorTransform) {
                handleProcessorStage((ProcessorTransform) transform);
            } else if (transform instanceof FilterTransform) {
                handleFilter((FilterTransform) transform);
            } else if (transform instanceof MapTransform) {
                handleMap((MapTransform) transform);
            } else if (transform instanceof FlatMapTransform) {
                handleFlatMap((FlatMapTransform) transform);
            } else if (transform instanceof GroupTransform) {
                handleGroup((GroupTransform) transform);
            } else if (transform instanceof CoGroupTransform) {
                handleCoGroup((CoGroupTransform) transform);
            } else if (transform instanceof HashJoinTransform) {
                handleHashJoin((HashJoinTransform) transform);
            } else if (transform instanceof PeekTransform) {
                handlePeek((PeekTransform) transform);
            } else if (transform instanceof SinkTransform) {
                handleSink((SinkTransform) transform);
            } else {
                throw new IllegalArgumentException("Unknown transform " + transform);
            }
        }
        return dag;
    }

    private static void validateNoLeakage(Map<Transform, List<Transform>> adjacencyMap) {
        List<Transform> leakages = adjacencyMap
                .entrySet().stream()
                .filter(e -> e.getValue().isEmpty())
                .map(Entry::getKey)
                .collect(toList());
        if (!leakages.isEmpty()) {
            throw new IllegalArgumentException("These transforms have nothing attached to them: " + leakages);
        }
    }

    private static void propagateEmitsJetEvents(Transform t, Map<Transform, List<Transform>> adjacencyMap) {
        if (!t.emitsJetEvents()) {
            return;
        }
        List<Transform> downstream = adjacencyMap.get(t);
        for (Transform d : downstream) {
            if (d.emitsJetEvents()) {
                continue;
            }
            d.setEmitsJetEvents(true);
            propagateEmitsJetEvents(d, adjacencyMap);
        }
    }

    private void handleSource(BatchSourceTransform source) {
        addVertex(source, vertexName(source.name(), ""), source.metaSupplier);
    }

    private void handleStreamSource(StreamSourceTransform source) {
        addVertex(source, vertexName(source.name(), ""), source.metaSupplier);
    }

    private void handleProcessorStage(ProcessorTransform procTransform) {
        PlannerVertex pv = addVertex(procTransform, vertexName(procTransform.name(), ""), procTransform.procSupplier);
        addEdges(procTransform, pv.v);
    }

    private void handleMap(MapTransform map) {
        PlannerVertex pv = addVertex(map, vertexName(map.name(), ""), mapP(map.mapFn));
        addEdges(map, pv.v);
    }

    private void handleFilter(FilterTransform filter) {
        PlannerVertex pv = addVertex(filter, vertexName(filter.name(), ""), filterP(filter.filterFn));
        addEdges(filter, pv.v);
    }

    private void handleFlatMap(FlatMapTransform flatMap) {
        PlannerVertex pv = addVertex(flatMap, vertexName(flatMap.name(), ""), flatMapP(flatMap.flatMapFn()));
        addEdges(flatMap, pv.v);
    }

    //                       --------
    //                      | source |
    //                       --------
    //                           |
    //                      partitioned
    //                           v
    //                  -------------------
    //                 | accumulatebyKeyP  |
    //                  -------------------
    //                           |
    //                      distributed
    //                      partitioned
    //                           v
    //                   ----------------
    //                  | combineByKeyP  |
    //                   ----------------
    private void handleGroup(GroupTransform<Object, Object, Object, Object, Object> xform) {
        if (xform.wDef != null) {
            handleWindowedGroup(xform);
            return;
        }
        String namePrefix = vertexName(xform.name(), "-stage");
        Vertex v1 = dag.newVertex(namePrefix + '1', accumulateByKeyP(
                xform.keyFn, xform.aggrOp.withFinishFn(identity())));
        PlannerVertex pv2 = addVertex(xform, namePrefix + '2', combineByKeyP(xform.aggrOp));
        addEdges(xform, v1, e -> e.partitioned(xform.keyFn, HASH_CODE));
        dag.edge(between(v1, pv2.v).distributed().partitioned(entryKey()));
    }

    private void handleWindowedGroup(
            GroupTransform<Object, Object, Object, Object, Object> xform
    ) {
        WindowDefinition wDef = requireNonNull(xform.wDef);
        switch (wDef.kind()) {
            case TUMBLING:
            case SLIDING:
                handleSlidingWindow(xform, wDef.downcast());
                return;
            case SESSION:
                handleSessionWindow(xform, wDef.downcast());
                return;
            default:
                throw new IllegalArgumentException("Unknown window definition " + wDef.kind());
        }
    }

    //                       --------
    //                      | source |
    //                       --------
    //                           |
    //                      partitioned
    //                           v
    //                 --------------------
    //                | accumulatebyFrameP |
    //                 --------------------
    //                           |
    //                      distributed
    //                      partitioned
    //                           v
    //                   -----------------
    //                  | combineByFrameP |
    //                   -----------------
    private void handleSlidingWindow(
            GroupTransform<Object, Object, Object, Object, Object> xform,
            SlidingWindowDef wDef
    ) {
        String namePrefix = vertexName("sliding-window", "-stage");
        SlidingWindowPolicy winPolicy = wDef.toSlidingWindowPolicy();
        Vertex v1 = dag.newVertex(namePrefix + '1', accumulateByFrameP(
                xform.keyFn,
                (TimestampedEntry<Object, Object> e) -> e.getTimestamp(),
                TimestampKind.EVENT,
                winPolicy,
                xform.aggrOp.withFinishFn(identity())));
        PlannerVertex pv2 = addVertex(xform, namePrefix + '2',
                combineToSlidingWindowP(winPolicy, xform.aggrOp));
        addEdges(xform, v1, e -> e.partitioned(xform.keyFn, HASH_CODE));
        dag.edge(between(v1, pv2.v).distributed().partitioned(entryKey()));
    }

    private void handleSessionWindow(
            GroupTransform<Object, Object, Object, Object, Object> xform,
            SessionWindowDef wDef
    ) {
        PlannerVertex pv = addVertex(xform, vertexName("session-window", ""), aggregateToSessionWindowP(
                wDef.sessionTimeout(),
                x -> 0L,
                xform.keyFn,
                xform.aggrOp
        ));
        addEdges(xform, pv.v, e -> e.partitioned(xform.keyFn));
    }

    //           ----------       ----------         ----------
    //          | source-1 |     | source-2 |  ...  | source-n |
    //           ----------       ----------         ----------
    //               |                 |                  |
    //          partitioned       partitioned        partitioned
    //               \------------v    v    v------------/
    //                       --------------------
    //                      | coAccumulateByKeyP |
    //                       --------------------
    //                                 |
    //                            distributed
    //                            partitioned
    //                                 v
    //                          ---------------
    //                         | combineByKeyP |
    //                          ---------------
    private void handleCoGroup(CoGroupTransform<Object, Object, Object, Object> xform) {
        if (xform.wDef != null) {
            handleWindowedCoGroup(xform);
            return;
        }
        List<DistributedFunction<?, ?>> groupKeyFns = xform.groupKeyFns;
        String namePrefix = vertexName(xform.name(), "-stage");
        Vertex v1 = dag.newVertex(namePrefix + '1',
                coAccumulateByKeyP(groupKeyFns, xform.aggrOp.withFinishFn(identity())));
        PlannerVertex pv2 = addVertex(xform, namePrefix + '2',
                combineByKeyP(xform.aggrOp));
        addEdges(xform, v1, (e, ord) -> e.partitioned(groupKeyFns.get(ord), HASH_CODE));
        dag.edge(between(v1, pv2.v).distributed().partitioned(entryKey()));
    }

    private void handleWindowedCoGroup(CoGroupTransform<Object, Object, Object, Object> xform) {
        WindowDefinition wDef = requireNonNull(xform.wDef);
        switch (wDef.kind()) {
            case TUMBLING:
            case SLIDING:
                handleSlidingCoWindow(xform, wDef.downcast());
                return;
            case SESSION:
                handleSessionCoWindow(xform, wDef.downcast());
                return;
            default:
                throw new IllegalArgumentException("Unknown window definition " + wDef.kind());
        }
    }

    private void handleSlidingCoWindow(
            CoGroupTransform<Object, Object, Object, Object> xform,
            SlidingWindowDef wDef
    ) {
        throw new UnsupportedOperationException("Windowed co-grouping not yet implemented");
    }

    private void handleSessionCoWindow(
            CoGroupTransform<Object, Object, Object, Object> xform,
            SessionWindowDef downcast
    ) {
        throw new UnsupportedOperationException("Windowed co-grouping not yet implemented");
    }

    //         ---------           ----------           ----------
    //        | primary |         | joined-1 |         | joined-2 |
    //         ---------           ----------           ----------
    //             |                   |                     |
    //             |              distributed          distributed
    //             |               broadcast            broadcast
    //             |                   v                     v
    //             |             -------------         -------------
    //             |            | collector-1 |       | collector-2 |
    //             |             -------------         -------------
    //             |                   |                     |
    //             |                 local                 local
    //        distributed          broadcast             broadcast
    //        partitioned         prioritized           prioritized
    //         ordinal 0           ordinal 1             ordinal 2
    //             \                   |                     |
    //              ----------------\  |   /----------------/
    //                              v  v  v
    //                              --------
    //                             | joiner |
    //                              --------
    private void handleHashJoin(HashJoinTransform<?, ?> hashJoin) {
        String namePrefix = vertexName(hashJoin.name(), "");
        PlannerVertex primary = xform2vertex.get(hashJoin.upstream().get(0));
        List<Function<Object, Object>> keyFns = (List<Function<Object, Object>>) (List)
                hashJoin.clauses.stream()
                        .map(JoinClause::leftKeyFn)
                        .collect(toList());
        Vertex joiner = addVertex(hashJoin, namePrefix + "joiner",
                () -> new HashJoinP(keyFns, hashJoin.tags, hashJoin.mapToOutputFn)).v;
        dag.edge(from(primary.v, primary.availableOrdinal++).to(joiner, 0));

        String collectorName = namePrefix + "collector-";
        int collectorOrdinal = 1;
        for (Transform fromTransform : tailList(hashJoin.upstream())) {
            PlannerVertex fromPv = xform2vertex.get(fromTransform);
            JoinClause<?, ?, ?, ?> clause = hashJoin.clauses.get(collectorOrdinal - 1);
            DistributedFunction<Object, Object> getKeyFn =
                    (DistributedFunction<Object, Object>) clause.rightKeyFn();
            DistributedFunction<Object, Object> projectFn =
                    (DistributedFunction<Object, Object>) clause.rightProjectFn();
            Vertex collector = dag.newVertex(collectorName + collectorOrdinal,
                    () -> new HashJoinCollectP(getKeyFn, projectFn));
            collector.localParallelism(1);
            dag.edge(from(fromPv.v, fromPv.availableOrdinal++)
                    .to(collector, 0)
                    .distributed().broadcast());
            dag.edge(from(collector, 0)
                    .to(joiner, collectorOrdinal)
                    .broadcast().priority(-1));
            collectorOrdinal++;
        }
    }

    private void handlePeek(PeekTransform peekTransform) {
        PlannerVertex peekedPv = xform2vertex.get(peekTransform.upstream().get(0));
        // Peeking transform doesn't add a vertex, so point to the upstream pipeline's
        // vertex:
        xform2vertex.put(peekTransform, peekedPv);
        peekedPv.v.updateMetaSupplier(sup ->
                peekOutputP(peekTransform.toStringFn, peekTransform.shouldLogFn, sup));
    }

    private void handleSink(SinkTransform sink) {
        PlannerVertex pv = addVertex(sink, vertexName(sink.name(), ""), sink.metaSupplier);
        addEdges(sink, pv.v);
    }

    private PlannerVertex addVertex(Transform transform, String name, DistributedSupplier<Processor> procSupplier) {
        return addVertex(transform, name, ProcessorMetaSupplier.of(procSupplier));
    }

    private PlannerVertex addVertex(Transform transform, String name, ProcessorMetaSupplier metaSupplier) {
        PlannerVertex pv = new PlannerVertex(dag.newVertex(name, metaSupplier));
        xform2vertex.put(transform, pv);
        return pv;
    }

    private void addEdges(Transform transform, Vertex toVertex, BiConsumer<Edge, Integer> configureEdgeFn) {
        int destOrdinal = 0;
        for (Transform fromTransform : transform.upstream()) {
            PlannerVertex fromPv = xform2vertex.get(fromTransform);
            Edge edge = from(fromPv.v, fromPv.availableOrdinal++).to(toVertex, destOrdinal);
            dag.edge(edge);
            configureEdgeFn.accept(edge, destOrdinal);
            destOrdinal++;
        }
    }

    private void addEdges(Transform transform, Vertex toVertex, Consumer<Edge> configureEdgeFn) {
        addEdges(transform, toVertex, (e, ord) -> configureEdgeFn.accept(e));
    }

    private void addEdges(Transform transform, Vertex toVertex) {
        addEdges(transform, toVertex, e -> { });
    }

    private String vertexName(@Nonnull String name, @Nonnull String suffix) {
        for (int index = 1; ; index++) {
            String candidate = name
                    + (index == 1 ? "" : "-" + index)
                    + suffix;
            if (vertexNames.add(candidate)) {
                return candidate;
            }
        }
    }

    private static <E> List<E> tailList(List<E> list) {
        return list.subList(1, list.size());
    }

    private static class PlannerVertex {
        Vertex v;

        int availableOrdinal;

        PlannerVertex(Vertex v) {
            this.v = v;
        }

        @Override
        public String toString() {
            return v.toString();
        }
    }
}
