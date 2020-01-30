/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Edge.RoutingPolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.ConcurrentInboundEdgeStream;
import com.hazelcast.jet.impl.execution.ConveyorCollector;
import com.hazelcast.jet.impl.execution.ConveyorCollectorWithPartition;
import com.hazelcast.jet.impl.execution.InboundEdgeStream;
import com.hazelcast.jet.impl.execution.OutboundCollector;
import com.hazelcast.jet.impl.execution.OutboundEdgeStream;
import com.hazelcast.jet.impl.execution.ProcessorTasklet;
import com.hazelcast.jet.impl.execution.ReceiverTasklet;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.execution.SnapshotContext;
import com.hazelcast.jet.impl.execution.StoreSnapshotTasklet;
import com.hazelcast.jet.impl.execution.Tasklet;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet.config.EdgeConfig.DEFAULT_QUEUE_SIZE;
import static com.hazelcast.jet.impl.execution.OutboundCollector.compositeCollector;
import static com.hazelcast.jet.impl.execution.TaskletExecutionService.TASKLET_INIT_CLOSE_EXECUTOR_NAME;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ImdgUtil.readList;
import static com.hazelcast.jet.impl.util.ImdgUtil.writeList;
import static com.hazelcast.jet.impl.util.Util.getJetInstance;
import static com.hazelcast.jet.impl.util.Util.memoize;
import static com.hazelcast.jet.impl.util.Util.sanitizeLoggerNamePart;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ExecutionPlan implements IdentifiedDataSerializable {

    // use same size as DEFAULT_QUEUE_SIZE from Edges. In the future we might
    // want to make this configurable
    private static final int SNAPSHOT_QUEUE_SIZE = DEFAULT_QUEUE_SIZE;

    private final List<Tasklet> tasklets = new ArrayList<>();
    /**
     * dest vertex id --> dest ordinal --> sender addr -> receiver tasklet
     */
    private final Map<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> receiverMap = new HashMap<>();
    /**
     * dest vertex id --> dest ordinal --> dest addr --> sender tasklet
     */
    private final Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap = new HashMap<>();

    /**
     * Snapshot of partition table used to route items on partitioned edges
     */
    private Address[] partitionOwners;

    private JobConfig jobConfig;
    private List<VertexDef> vertices = new ArrayList<>();

    private int memberIndex;
    private int memberCount;

    private final Map<String, ConcurrentConveyor<Object>[]> localConveyorMap = new HashMap<>();
    private final Map<String, Map<Address, ConcurrentConveyor<Object>>> edgeSenderConveyorMap = new HashMap<>();
    private final List<Processor> processors = new ArrayList<>();

    private PartitionArrangement ptionArrgmt;

    private NodeEngineImpl nodeEngine;
    private long executionId;
    private long lastSnapshotId;

    // list of unique remote members
    private final Supplier<Set<Address>> remoteMembers = memoize(() ->
            Arrays.stream(partitionOwners)
                  .filter(a -> !a.equals(nodeEngine.getThisAddress()))
                  .collect(Collectors.toSet())
    );

    ExecutionPlan() {
    }

    ExecutionPlan(Address[] partitionOwners, JobConfig jobConfig, long lastSnapshotId,
                  int memberIndex, int memberCount) {
        this.partitionOwners = partitionOwners;
        this.jobConfig = jobConfig;
        this.lastSnapshotId = lastSnapshotId;
        this.memberIndex = memberIndex;
        this.memberCount = memberCount;
    }

    public void initialize(
            NodeEngine nodeEngine, long jobId, long executionId, SnapshotContext snapshotContext,
            ConcurrentHashMap<String, File> tempDirectories
    ) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.executionId = executionId;
        initProcSuppliers(jobId, executionId, tempDirectories);
        initDag();

        this.ptionArrgmt = new PartitionArrangement(partitionOwners, nodeEngine.getThisAddress());
        JetInstance instance = getJetInstance(nodeEngine);
        Set<Integer> higherPriorityVertices = VertexDef.getHigherPriorityVertices(vertices);
        for (VertexDef vertex : vertices) {
            Collection<? extends Processor> processors = createProcessors(vertex, vertex.localParallelism());

            // create StoreSnapshotTasklet and the queues to it
            QueuedPipe<Object>[] snapshotQueues = new QueuedPipe[vertex.localParallelism()];
            Arrays.setAll(snapshotQueues, i -> new OneToOneConcurrentArrayQueue<>(SNAPSHOT_QUEUE_SIZE));
            ConcurrentConveyor<Object> ssConveyor = ConcurrentConveyor.concurrentConveyor(null, snapshotQueues);
            StoreSnapshotTasklet ssTasklet = new StoreSnapshotTasklet(snapshotContext,
                    new ConcurrentInboundEdgeStream(ssConveyor, 0, 0, true,
                            "ssFrom:" + vertex.name()),
                    new AsyncSnapshotWriterImpl(nodeEngine, snapshotContext, vertex.name(), memberIndex, memberCount),
                    nodeEngine.getLogger(StoreSnapshotTasklet.class.getName() + "."
                            + sanitizeLoggerNamePart(vertex.name())),
                    vertex.name(), higherPriorityVertices.contains(vertex.vertexId()));
            tasklets.add(ssTasklet);

            int localProcessorIdx = 0;
            for (Processor processor : processors) {
                int globalProcessorIndex = memberIndex * vertex.localParallelism() + localProcessorIdx;
                String loggerName = createLoggerName(
                        processor.getClass().getName(),
                        jobConfig.getName(),
                        vertex.name(),
                        globalProcessorIndex
                );
                ProcCtx context = new ProcCtx(
                        instance,
                        jobId,
                        executionId,
                        getJobConfig(),
                        nodeEngine.getLogger(loggerName),
                        vertex.name(),
                        localProcessorIdx,
                        globalProcessorIndex,
                        jobConfig.getProcessingGuarantee(),
                        vertex.localParallelism(),
                        memberIndex,
                        memberCount,
                        tempDirectories
                );

                // createOutboundEdgeStreams() populates localConveyorMap and edgeSenderConveyorMap.
                // Also populates instance fields: senderMap, receiverMap, tasklets.
                List<OutboundEdgeStream> outboundStreams = createOutboundEdgeStreams(
                        vertex, localProcessorIdx
                );
                List<InboundEdgeStream> inboundStreams = createInboundEdgeStreams(
                        vertex, localProcessorIdx, globalProcessorIndex
                );

                OutboundCollector snapshotCollector = new ConveyorCollector(ssConveyor, localProcessorIdx, null);

                ProcessorTasklet processorTasklet = new ProcessorTasklet(context,
                        nodeEngine.getExecutionService().getExecutor(TASKLET_INIT_CLOSE_EXECUTOR_NAME),
                        nodeEngine.getSerializationService(), processor, inboundStreams, outboundStreams, snapshotContext,
                        snapshotCollector);
                tasklets.add(processorTasklet);
                this.processors.add(processor);
                localProcessorIdx++;
            }
        }
        List<ReceiverTasklet> allReceivers = receiverMap.values().stream()
                                                        .flatMap(o -> o.values().stream())
                                                        .flatMap(a -> a.values().stream())
                                                        .collect(toList());

        tasklets.addAll(allReceivers);
    }

    public static String createLoggerName(
            String processorClassName, String jobName, String vertexName, int processorIndex
    ) {
        vertexName = sanitizeLoggerNamePart(vertexName);
        if (StringUtil.isNullOrEmptyAfterTrim(jobName)) {
            return processorClassName + '.' + vertexName + '#' + processorIndex;
        } else {
            return processorClassName + '.' + jobName.trim() + "/" + vertexName + '#' + processorIndex;
        }
    }

    public List<ProcessorSupplier> getProcessorSuppliers() {
        return toList(vertices, VertexDef::processorSupplier);
    }

    public Map<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> getReceiverMap() {
        return receiverMap;
    }

    public Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> getSenderMap() {
        return senderMap;
    }

    public List<Tasklet> getTasklets() {
        return tasklets;
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    void addVertex(VertexDef vertex) {
        vertices.add(vertex);
    }

    // Implementation of IdentifiedDataSerializable

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.EXECUTION_PLAN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeList(out, vertices);
        out.writeInt(partitionOwners.length);
        out.writeLong(lastSnapshotId);
        for (Address address : partitionOwners) {
            out.writeObject(address);
        }
        out.writeObject(jobConfig);
        out.writeInt(memberIndex);
        out.writeInt(memberCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vertices = readList(in);
        int len = in.readInt();
        partitionOwners = new Address[len];
        lastSnapshotId = in.readLong();
        for (int i = 0; i < len; i++) {
            partitionOwners[i] = in.readObject();
        }
        jobConfig = in.readObject();
        memberIndex = in.readInt();
        memberCount = in.readInt();
    }

    // End implementation of IdentifiedDataSerializable

    private void initProcSuppliers(long jobId, long executionId, ConcurrentHashMap<String, File> tempDirectories) {
        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);

        for (VertexDef vertex : vertices) {
            ProcessorSupplier supplier = vertex.processorSupplier();
            ILogger logger = nodeEngine.getLogger(supplier.getClass().getName() + '.'
                    + vertex.name() + "#ProcessorSupplier");
            try {
                supplier.init(new ProcSupplierCtx(
                        service.getJetInstance(),
                        jobId,
                        executionId,
                        jobConfig,
                        logger,
                        vertex.name(),
                        vertex.localParallelism(),
                        vertex.localParallelism() * memberCount,
                        memberIndex,
                        memberCount,
                        jobConfig.getProcessingGuarantee(),
                        tempDirectories
                ));
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }
    }

    private void initDag() {
        final Map<Integer, VertexDef> vMap = vertices.stream().collect(toMap(VertexDef::vertexId, v -> v));
        for (VertexDef v : vertices) {
            v.inboundEdges().forEach(e -> e.initTransientFields(vMap, v, false));
            v.outboundEdges().forEach(e -> e.initTransientFields(vMap, v, true));
        }
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        vertices.stream()
                .map(VertexDef::outboundEdges)
                .flatMap(List::stream)
                .map(EdgeDef::partitioner)
                .filter(Objects::nonNull)
                .forEach(p -> p.init(partitionService::getPartitionId));
    }

    private static Collection<? extends Processor> createProcessors(VertexDef vertexDef, int parallelism) {
        final Collection<? extends Processor> processors = vertexDef.processorSupplier().get(parallelism);
        if (processors.size() != parallelism) {
            throw new JetException("ProcessorSupplier failed to return the requested number of processors." +
                    " Requested: " + parallelism + ", returned: " + processors.size());
        }
        return processors;
    }

    /**
     * Populates {@code localConveyorMap}, {@code edgeSenderConveyorMap}.
     * Populates {@link #senderMap} and {@link #tasklets} fields.
     */
    private List<OutboundEdgeStream> createOutboundEdgeStreams(
            VertexDef srcVertex, int processorIdx
    ) {
        final List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
        for (EdgeDef edge : srcVertex.outboundEdges()) {
            Map<Address, ConcurrentConveyor<Object>> memberToSenderConveyorMap = null;
            if (edge.isDistributed()) {
                memberToSenderConveyorMap = memberToSenderConveyorMap(edgeSenderConveyorMap, edge);
            }
            outboundStreams.add(createOutboundEdgeStream(edge, processorIdx, memberToSenderConveyorMap));
        }
        return outboundStreams;
    }

    /**
     * Creates (if absent) for the given edge one sender tasklet per remote member,
     * each with a single conveyor with a number of producer queues feeding it.
     * Populates the {@link #senderMap} and {@link #tasklets} fields.
     */
    private Map<Address, ConcurrentConveyor<Object>> memberToSenderConveyorMap(
            Map<String, Map<Address, ConcurrentConveyor<Object>>> edgeSenderConveyorMap, EdgeDef edge
    ) {
        assert edge.isDistributed() : "Edge is not distributed";
        return edgeSenderConveyorMap.computeIfAbsent(edge.edgeId(), x -> {
            final Map<Address, ConcurrentConveyor<Object>> addrToConveyor = new HashMap<>();
            for (Address destAddr : remoteMembers.get()) {
                final ConcurrentConveyor<Object> conveyor = createConveyorArray(
                        1, edge.sourceVertex().localParallelism(), edge.getConfig().getQueueSize())[0];
                final ConcurrentInboundEdgeStream inboundEdgeStream = newEdgeStream(edge, conveyor,
                        "sender-toVertex:" + edge.destVertex().name() + "-toMember:"
                                + destAddr.toString().replace('.', '-'));
                final int destVertexId = edge.destVertex().vertexId();
                final SenderTasklet t = new SenderTasklet(inboundEdgeStream, nodeEngine, destAddr,
                        destVertexId, edge.getConfig().getPacketSizeLimit(), executionId,
                        edge.sourceVertex().name(), edge.sourceOrdinal()
                );
                senderMap.computeIfAbsent(destVertexId, xx -> new HashMap<>())
                         .computeIfAbsent(edge.destOrdinal(), xx -> new HashMap<>())
                         .put(destAddr, t);
                tasklets.add(t);
                addrToConveyor.put(destAddr, conveyor);
            }
            return addrToConveyor;
        });
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentConveyor<Object>[] createConveyorArray(int count, int queueCount, int queueSize) {
        ConcurrentConveyor<Object>[] concurrentConveyors = new ConcurrentConveyor[count];
        Arrays.setAll(concurrentConveyors, i -> {
            QueuedPipe<Object>[] queues = new QueuedPipe[queueCount];
            Arrays.setAll(queues, j -> new OneToOneConcurrentArrayQueue<>(queueSize));
            return concurrentConveyor(null, queues);
        });
        return concurrentConveyors;
    }

    private OutboundEdgeStream createOutboundEdgeStream(
            EdgeDef edge, int processorIndex, Map<Address, ConcurrentConveyor<Object>> senderConveyorMap
    ) {
        final int totalPtionCount = nodeEngine.getPartitionService().getPartitionCount();
        OutboundCollector[] outboundCollectors = createOutboundCollectors(
                edge, processorIndex, senderConveyorMap
        );
        OutboundCollector compositeCollector = compositeCollector(outboundCollectors, edge, totalPtionCount);
        return new OutboundEdgeStream(edge.sourceOrdinal(), compositeCollector);
    }

    private OutboundCollector[] createOutboundCollectors(
            EdgeDef edge, int processorIndex, Map<Address, ConcurrentConveyor<Object>> senderConveyorMap
    ) {
        final int upstreamParallelism = edge.sourceVertex().localParallelism();
        final int downstreamParallelism = edge.destVertex().localParallelism();
        final int numRemoteMembers = ptionArrgmt.remotePartitionAssignment.get().size();
        final int queueSize = edge.getConfig().getQueueSize();

        if (edge.routingPolicy() == RoutingPolicy.ISOLATED) {
            if (downstreamParallelism < upstreamParallelism) {
                throw new IllegalArgumentException(String.format(
                        "The edge %s specifies the %s routing policy, but the downstream vertex" +
                                " parallelism (%d) is less than the upstream vertex parallelism (%d)",
                        edge, RoutingPolicy.ISOLATED.name(), downstreamParallelism, upstreamParallelism));
            }
            if (edge.isDistributed()) {
                throw new IllegalArgumentException("Isolated edges must be local: " + edge);
            }

            // there is only one producer per consumer for a one to many edge, so queueCount is always 1
            ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.computeIfAbsent(edge.edgeId(),
                    e -> createConveyorArray(downstreamParallelism, 1, queueSize));
            return IntStream.range(0, downstreamParallelism)
                            .filter(i -> i % upstreamParallelism == processorIndex)
                            .mapToObj(i -> new ConveyorCollector(localConveyors[i], 0, null))
                            .toArray(OutboundCollector[]::new);
        }

        /*
         * Each edge is represented by an array of conveyors between the producers and consumers
         * There are as many conveyors as there are consumers.
         * Each conveyor has one queue per producer.
         *
         * For a distributed edge, there is one additional producer per member represented
         * by the ReceiverTasklet.
         */
        final int[][] ptionsPerProcessor = getPartitionDistribution(edge, downstreamParallelism);
        final ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.computeIfAbsent(edge.edgeId(),
                e -> {
                    int queueCount = upstreamParallelism + (edge.isDistributed() ? numRemoteMembers : 0);
                    return createConveyorArray(downstreamParallelism, queueCount, queueSize);
                });
        final OutboundCollector[] localCollectors = new OutboundCollector[downstreamParallelism];
        Arrays.setAll(localCollectors, n ->
                new ConveyorCollector(localConveyors[n], processorIndex, ptionsPerProcessor[n]));

        // in a local edge, we only have the local collectors.
        if (!edge.isDistributed()) {
            return localCollectors;
        }

        // in a distributed edge, allCollectors[0] is the composite of local collectors, and
        // allCollectors[n] where n > 0 is a collector pointing to a remote member _n_.
        final int totalPtionCount = nodeEngine.getPartitionService().getPartitionCount();
        final OutboundCollector[] allCollectors;
        createIfAbsentReceiverTasklet(edge, ptionsPerProcessor, totalPtionCount);

        // assign remote partitions to outbound data collectors
        final Map<Address, int[]> memberToPartitions = ptionArrgmt.remotePartitionAssignment.get();
        allCollectors = new OutboundCollector[memberToPartitions.size() + 1];
        allCollectors[0] = compositeCollector(localCollectors, edge, totalPtionCount);
        int index = 1;
        for (Map.Entry<Address, int[]> entry : memberToPartitions.entrySet()) {
            allCollectors[index++] = new ConveyorCollectorWithPartition(senderConveyorMap.get(entry.getKey()),
                    processorIndex, entry.getValue());
        }
        return allCollectors;
    }

    private int[][] getPartitionDistribution(EdgeDef edge, int downstreamParallelism) {
        return edge.routingPolicy().equals(RoutingPolicy.PARTITIONED) ?
                ptionArrgmt.assignPartitionsToProcessors(downstreamParallelism, edge.isDistributed())
                : new int[downstreamParallelism][];
    }

    private void createIfAbsentReceiverTasklet(EdgeDef edge, int[][] ptionsPerProcessor, int totalPtionCount) {
        final ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.get(edge.edgeId());

        receiverMap.computeIfAbsent(edge.destVertex().vertexId(), x -> new HashMap<>())
                   .computeIfAbsent(edge.destOrdinal(), x -> {
                       Map<Address, ReceiverTasklet> addrToTasklet = new HashMap<>();
                       //create a receiver per address
                       int offset = 0;
                       for (Address addr : ptionArrgmt.remotePartitionAssignment.get().keySet()) {
                           final OutboundCollector[] collectors = new OutboundCollector[ptionsPerProcessor.length];
                           // assign the queues starting from end
                           final int queueOffset = --offset;
                           Arrays.setAll(collectors, n -> new ConveyorCollector(
                                   localConveyors[n], localConveyors[n].queueCount() + queueOffset,
                                   ptionsPerProcessor[n]));
                           final OutboundCollector collector = compositeCollector(collectors, edge, totalPtionCount);
                           ReceiverTasklet receiverTasklet = new ReceiverTasklet(
                                   collector, edge.getConfig().getReceiveWindowMultiplier(),
                                   getConfig().getInstanceConfig().getFlowControlPeriodMs(),
                                   nodeEngine.getLoggingService(), addr, edge.destOrdinal(), edge.destVertex().name());
                           addrToTasklet.put(addr, receiverTasklet);
                       }
                       return addrToTasklet;
                   });
    }

    private JetConfig getConfig() {
        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        return service.getJetInstance().getConfig();
    }

    private List<InboundEdgeStream> createInboundEdgeStreams(VertexDef srcVertex, int localProcessorIdx,
                                                             int globalProcessorIdx) {
        final List<InboundEdgeStream> inboundStreams = new ArrayList<>();
        for (EdgeDef inEdge : srcVertex.inboundEdges()) {
            // each tasklet has one input conveyor per edge
            final ConcurrentConveyor<Object> conveyor = localConveyorMap.get(inEdge.edgeId())[localProcessorIdx];
            inboundStreams.add(newEdgeStream(inEdge, conveyor,
                    "inputTo:" + inEdge.destVertex().name() + '#' + globalProcessorIdx));
        }
        return inboundStreams;
    }

    private ConcurrentInboundEdgeStream newEdgeStream(EdgeDef inEdge, ConcurrentConveyor<Object> conveyor,
                                                      String debugName) {
        return new ConcurrentInboundEdgeStream(conveyor, inEdge.destOrdinal(), inEdge.priority(),
                jobConfig.getProcessingGuarantee() == ProcessingGuarantee.EXACTLY_ONCE,
                debugName);
    }

    public List<Processor> getProcessors() {
        return processors;
    }

    public long lastSnapshotId() {
        return lastSnapshotId;
    }

    public int getStoreSnapshotTaskletCount() {
        return (int) tasklets.stream()
                             .filter(t -> t instanceof StoreSnapshotTasklet)
                             .count();
    }

    public int getProcessorTaskletCount() {
        return (int) tasklets.stream()
                             .filter(t -> t instanceof ProcessorTasklet)
                             .count();
    }

    public int getHigherPriorityVertexCount() {
        return VertexDef.getHigherPriorityVertices(vertices).size();
    }

    // for test
    List<VertexDef> getVertices() {
        return vertices;
    }
}
