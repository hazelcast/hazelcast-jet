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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.Member;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.ProcessorMetaSupplier;
import com.hazelcast.jet2.ProcessorMetaSupplier.Context;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.jet2.impl.deployment.JetClassLoader;
import com.hazelcast.jet2.impl.deployment.ResourceStore;
import com.hazelcast.spi.NodeEngine;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class EngineContext {

    private final String name;
    private NodeEngine nodeEngine;
    private ExecutionService executionService;
    private ResourceStore resourceStore;
    private JetEngineConfig config;

    // Type of variable is CHM and not ConcurrentMap because we rely on specific semantics of computeIfAbsent.
    // ConcurrentMap.computeIfAbsent does not guarantee at most one computation per key.
    private ConcurrentHashMap<Long, ExecutionContext> executionContexts = new ConcurrentHashMap<>();

    public EngineContext(String name, NodeEngine nodeEngine, JetEngineConfig config) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.config = config;
        this.resourceStore = new ResourceStore(config.getResourceDirectory());
        final ClassLoader cl = AccessController.doPrivileged(
                (PrivilegedAction<ClassLoader>) () -> new JetClassLoader(resourceStore));
        this.executionService = new ExecutionService(nodeEngine.getHazelcastInstance(), name, config, cl);
    }

    public Map<Member, ExecutionPlan> newExecutionPlan(long executionId, DAG dag) {
        final List<Member> members = new ArrayList<>(nodeEngine.getClusterService().getMembers());
        final int clusterSize = members.size();
        final Map<Member, ExecutionPlan> plans = members.stream().collect(toMap(m -> m, m ->
                new ExecutionPlan(executionId)));
        final Map<String, Integer> vertexIdMap = assignVertexIds(dag);
        for (Entry<String, Integer> entry : vertexIdMap.entrySet()) {
            final Vertex vertex = dag.getVertex(entry.getKey());
            final int vertexId = entry.getValue();
            final int perNodeParallelism = getParallelism(vertex, config);
            final int totalParallelism = perNodeParallelism * clusterSize;
            final List<Edge> outboundEdges = dag.getOutboundEdges(vertex.getName());
            final List<Edge> inboundEdges = dag.getInboundEdges(vertex.getName());
            final ProcessorMetaSupplier supplier = vertex.getSupplier();
            supplier.init(Context.of(nodeEngine, totalParallelism, perNodeParallelism));

            final List<EdgeDef> outputs = outboundEdges.stream().map(edge -> {
                int otherEndId = vertexIdMap.get(edge.getDestination());
                return new EdgeDef(otherEndId, edge.getOutputOrdinal(), edge.getInputOrdinal(),
                        edge.getPriority(), isDistributed(edge), edge.getForwardingPattern(), edge.getPartitioner());
            }).collect(toList());

            final List<EdgeDef> inputs = inboundEdges.stream().map(edge -> {
                final int otherEndId = vertexIdMap.get(edge.getSource());
                return new EdgeDef(otherEndId, edge.getInputOrdinal(), edge.getInputOrdinal(),
                        edge.getPriority(), isDistributed(edge), edge.getForwardingPattern(), edge.getPartitioner());
            }).collect(toList());

            for (Entry<Member, ExecutionPlan> e : plans.entrySet()) {
                final ProcessorSupplier processorSupplier = supplier.get(e.getKey().getAddress());
                final VertexDef vertexDef = new VertexDef(vertexId, processorSupplier, perNodeParallelism);
                vertexDef.addOutputs(outputs);
                vertexDef.addInputs(inputs);
                e.getValue().addVertex(vertexDef);
            }
        }
        return plans;
    }

    public void createAndRegisterExecutionContext(ExecutionPlan plan) {
        executionContexts.computeIfAbsent(plan.getId(), k -> new ExecutionContext(this, plan));
    }

    public ExecutionContext getExecutionContext(long id) {
        return executionContexts.get(id);
    }

    public String getName() {
        return name;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public ExecutionService getExecutionService() {
        return executionService;
    }

    public ResourceStore getResourceStore() {
        return resourceStore;
    }

    public JetEngineConfig getConfig() {
        return config;
    }

    public void destroy() {
        resourceStore.destroy();
        executionService.shutdown();
    }

    private boolean isDistributed(Edge edge) {
        return edge.isDistributed() && nodeEngine.getClusterService().getSize() > 1;
    }

    private static Map<String, Integer> assignVertexIds(DAG dag) {
        Map<String, Integer> vertexIdMap = new LinkedHashMap<>();
        final int[] vertexId = {0};
        dag.forEach(v -> vertexIdMap.put(v.getName(), vertexId[0]++));
        return vertexIdMap;
    }

    private static int getParallelism(Vertex vertex, JetEngineConfig config) {
        return vertex.getParallelism() != -1 ? vertex.getParallelism() : config.getParallelism();
    }
}
