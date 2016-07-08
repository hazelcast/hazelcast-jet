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

package com.hazelcast.jet.impl.container.task.processors.shuffling;


import com.hazelcast.jet.PartitionIdAware;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.actor.ObjectConsumer;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.container.ApplicationMaster;
import com.hazelcast.jet.impl.container.ContainerContext;
import com.hazelcast.jet.impl.container.ContainerTask;
import com.hazelcast.jet.impl.container.ProcessingContainer;
import com.hazelcast.jet.impl.container.task.processors.ConsumerTaskProcessor;
import com.hazelcast.jet.impl.strategy.CalculationStrategyImpl;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.processor.ContainerProcessor;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.jet.strategy.CalculationStrategyAware;
import com.hazelcast.jet.strategy.ShufflingStrategy;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.HashUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ShuffledConsumerTaskProcessor extends ConsumerTaskProcessor {
    private final int chunkSize;
    private final boolean receiver;
    private final NodeEngine nodeEngine;
    private final DataWriter[] sendersArray;
    private final boolean hasLocalConsumers;
    private final ObjectConsumer[] shuffledConsumers;
    private final Address[] nonPartitionedAddresses;
    private final ObjectConsumer[] nonPartitionedWriters;
    private final CalculationStrategy[] calculationStrategies;
    private final Map<Address, DataWriter> senders = new HashMap<Address, DataWriter>();
    private final Map<CalculationStrategy, Map<Integer, List<ObjectConsumer>>> partitionedWriters;
    private final ObjectConsumer[] markers;
    private final Map<ObjectConsumer, Integer> markersCache = new IdentityHashMap<ObjectConsumer, Integer>();
    private final ContainerContext containerContext;
    private int lastConsumedSize;
    private boolean chunkInProgress;
    private boolean localSuccess;

    public ShuffledConsumerTaskProcessor(ObjectConsumer[] consumers,
                                         ContainerProcessor processor,
                                         ContainerContext containerContext,
                                         ProcessorContext processorContext,
                                         int taskID) {
        this(consumers, processor, containerContext, processorContext, taskID, false);
    }

    public ShuffledConsumerTaskProcessor(ObjectConsumer[] consumers,
                                         ContainerProcessor processor,
                                         ContainerContext containerContext,
                                         ProcessorContext processorContext,
                                         int taskID,
                                         boolean receiver) {
        super(filterConsumers(consumers, false), processor, containerContext, processorContext);
        this.nodeEngine = containerContext.getNodeEngine();
        this.containerContext = containerContext;

        initSenders(containerContext, taskID, receiver);

        this.sendersArray = this.senders.values().toArray(new DataWriter[this.senders.size()]);
        this.hasLocalConsumers = this.consumers.length > 0;

        this.shuffledConsumers = filterConsumers(consumers, true);
        this.markers = new ObjectConsumer[this.sendersArray.length + this.shuffledConsumers.length];

        initMarkers();

        this.partitionedWriters = new HashMap<CalculationStrategy, Map<Integer, List<ObjectConsumer>>>();

        Set<CalculationStrategy> strategies = new HashSet<CalculationStrategy>();
        List<ObjectConsumer> nonPartitionedConsumers = new ArrayList<ObjectConsumer>(this.shuffledConsumers.length);
        Set<Address> nonPartitionedAddresses = new HashSet<Address>(this.shuffledConsumers.length);

        // Process shuffled consumers
        initCalculationStrategies(
                strategies,
                nonPartitionedConsumers,
                nonPartitionedAddresses
        );

        this.receiver = receiver;
        this.calculationStrategies = strategies.toArray(new CalculationStrategy[strategies.size()]);
        this.nonPartitionedWriters = nonPartitionedConsumers.toArray(new ObjectConsumer[nonPartitionedConsumers.size()]);
        this.nonPartitionedAddresses = nonPartitionedAddresses.toArray(new Address[nonPartitionedAddresses.size()]);
        this.chunkSize = chunkSize(containerContext);

        resetState();
    }

    private static ObjectConsumer[] filterConsumers(ObjectConsumer[] consumers, boolean isShuffled) {
        List<ObjectConsumer> filtered = new ArrayList<ObjectConsumer>(consumers.length);

        for (ObjectConsumer consumer : consumers) {
            if (consumer.isShuffled() == isShuffled) {
                filtered.add(consumer);
            }
        }

        return filtered.toArray(new ObjectConsumer[filtered.size()]);
    }

    private int chunkSize(ContainerContext containerContext) {
        ApplicationConfig applicationConfig = containerContext.getApplicationContext().getApplicationConfig();
        return applicationConfig.getChunkSize();
    }

    private void initMarkers() {
        int position = 0;

        for (DataWriter sender : this.sendersArray) {
            this.markersCache.put(sender, position++);
        }
        for (ObjectConsumer shuffledConsumer : this.shuffledConsumers) {
            this.markersCache.put(shuffledConsumer, position++);
        }
    }

    private void initSenders(ContainerContext containerContext, int taskID, boolean receiver) {
        ApplicationMaster applicationMaster = containerContext.getApplicationContext().getApplicationMaster();

        ProcessingContainer processingContainer = applicationMaster.getContainerByVertex(containerContext.getVertex());
        ContainerTask containerTask = processingContainer.getTasksCache().get(taskID);

        if (!receiver) {
            for (Address address : applicationMaster.getApplicationContext().getSocketWriters().keySet()) {
                ShufflingSender sender = new ShufflingSender(containerContext, taskID, containerTask, address);
                this.senders.put(address, sender);
                applicationMaster.registerShufflingSender(taskID, containerContext, address, sender);
            }
        }
    }

    private void initCalculationStrategies(Set<CalculationStrategy> strategies,
                                           List<ObjectConsumer> nonPartitionedConsumers,
                                           Set<Address> nonPartitionedAddresses) {

        ApplicationMaster applicationMaster = this.containerContext.getApplicationContext().getApplicationMaster();
        Map<Address, Address> hzToJetAddressMapping = applicationMaster.getApplicationContext().getHzToJetAddressMapping();
        List<Integer> localPartitions = JetUtil.getLocalPartitions(nodeEngine);
        for (ObjectConsumer consumer : this.shuffledConsumers) {
            ShufflingStrategy shufflingStrategy = consumer.getShufflingStrategy();
            initConsumerCalculationStrategy(
                    strategies,
                    localPartitions,
                    nonPartitionedConsumers,
                    nonPartitionedAddresses,
                    hzToJetAddressMapping,
                    consumer,
                    shufflingStrategy
            );
        }
    }

    private void initConsumerCalculationStrategy(Set<CalculationStrategy> strategies,
                                                 List<Integer> localPartitions,
                                                 List<ObjectConsumer> nonPartitionedConsumers,
                                                 Set<Address> nonPartitionedAddresses,
                                                 Map<Address, Address> hzToJetAddressMapping,
                                                 ObjectConsumer consumer,
                                                 ShufflingStrategy shufflingStrategy) {
        if (shufflingStrategy != null) {
            Address[] addresses = shufflingStrategy.getShufflingAddress(this.containerContext);

            if (addresses != null) {
                for (Address address : addresses) {
                    if (address.equals(this.nodeEngine.getThisAddress())) {
                        nonPartitionedConsumers.add(
                                consumer
                        );
                    } else {
                        nonPartitionedAddresses.add(
                                hzToJetAddressMapping.get(address)
                        );
                    }
                }

                return;
            }
        }

        initConsumerPartitions(
                strategies,
                localPartitions,
                nonPartitionedConsumers,
                nonPartitionedAddresses,
                hzToJetAddressMapping,
                consumer
        );
    }

    private void initConsumerPartitions(Set<CalculationStrategy> strategies,
                                        List<Integer> localPartitions,
                                        List<ObjectConsumer> nonPartitionedConsumers,
                                        Set<Address> nonPartitionedAddresses,
                                        Map<Address, Address> hzToJetAddressMapping,
                                        ObjectConsumer consumer
    ) {
        CalculationStrategy calculationStrategy = new CalculationStrategyImpl(
                consumer.getHashingStrategy(),
                consumer.getPartitionStrategy(),
                this.containerContext
        );

        int partitionId;
        if (consumer instanceof DataWriter) {
            DataWriter writer = (DataWriter) consumer;

            if (!writer.isPartitioned()) {
                if (writer.getPartitionId() >= 0) {
                    processWriterPartition(
                            nonPartitionedConsumers,
                            nonPartitionedAddresses,
                            hzToJetAddressMapping,
                            writer
                    );
                } else {
                    nonPartitionedConsumers.add(
                            writer
                    );
                }

                return;
            }

            partitionId = writer.getPartitionId();
        } else {
            partitionId = -1;
        }

        strategies.add(calculationStrategy);
        Map<Integer, List<ObjectConsumer>> map = this.partitionedWriters.get(calculationStrategy);

        if (map == null) {
            map = new HashMap<>();
            this.partitionedWriters.put(calculationStrategy, map);
        }

        if (partitionId >= 0) {
            processPartition(map, partitionId).add(consumer);
        } else {
            for (Integer localPartition : localPartitions) {
                processPartition(map, localPartition).add(consumer);
            }
        }
    }

    private void processWriterPartition(List<ObjectConsumer> nonPartitionedConsumers,
                                        Set<Address> nonPartitionedAddresses,
                                        Map<Address, Address> hzToJetAddressMapping,
                                        DataWriter writer) {
        if (JetUtil.isPartitionLocal(nodeEngine, writer.getPartitionId())) {
            nonPartitionedConsumers.add(writer);
        } else {
            nonPartitionedAddresses.add(
                    hzToJetAddressMapping.get(
                            this.nodeEngine.getPartitionService().getPartitionOwner(writer.getPartitionId())
                    )
            );
        }
    }

    private List<ObjectConsumer> processPartition(Map<Integer, List<ObjectConsumer>> map, int partitionId) {
        List<ObjectConsumer> partitionOwnerWriters = map.get(partitionId);

        if (partitionOwnerWriters == null) {
            partitionOwnerWriters = new ArrayList<>();
            map.put(partitionId, partitionOwnerWriters);
        }

        return partitionOwnerWriters;
    }

    public void onOpen() {
        super.onOpen();

        for (ObjectConsumer objectConsumer : this.shuffledConsumers) {
            objectConsumer.open();
        }

        for (DataWriter tupleWriter : this.sendersArray) {
            tupleWriter.open();
        }

        reset();
    }

    public void onClose() {
        super.onClose();

        for (ObjectConsumer objectConsumer : this.shuffledConsumers) {
            objectConsumer.close();
        }

        for (DataWriter tupleWriter : this.sendersArray) {
            tupleWriter.close();
        }
    }

    private void resetState() {
        this.lastConsumedSize = 0;
        this.chunkInProgress = false;
        this.localSuccess = this.receiver || !this.hasLocalConsumers;
    }

    @Override
    public boolean onChunk(ProducerInputStream<Object> chunk) throws Exception {
        if (chunk.size() > 0) {
            boolean success = false;
            boolean consumed;

            /// Local consumers
            consumed = processLocalConsumers(chunk);

            // Shufflers
            boolean chunkPooled = this.lastConsumedSize >= chunk.size();

            if ((!chunkInProgress) && (!chunkPooled)) {
                this.lastConsumedSize = processShufflers(chunk, this.lastConsumedSize);
                chunkPooled = this.lastConsumedSize >= chunk.size();
                this.chunkInProgress = true;
                consumed = true;
            }

            if (this.chunkInProgress) {
                boolean flushed = processChunkProgress();
                consumed = true;

                if (flushed) {
                    this.chunkInProgress = false;
                }
            }

            /// Check if we are success with chunk
            if ((!this.chunkInProgress)
                    &&
                    (chunkPooled)
                    &&
                    (this.localSuccess)) {
                success = true;
                consumed = true;
            }

            if (success) {
                resetState();
            }

            this.consumed = consumed;
            return success;
        } else {
            this.consumed = false;
            return true;
        }
    }

    private boolean processLocalConsumers(ProducerInputStream<Object> chunk) throws Exception {
        boolean consumed = false;

        if (!this.receiver) {
            if ((this.hasLocalConsumers) && (!this.localSuccess)) {
                this.localSuccess = super.onChunk(chunk);
                consumed = super.consumed();
            }
        }
        return consumed;
    }

    private boolean processChunkProgress() {
        boolean flushed = true;

        for (DataWriter sender : this.sendersArray) {
            flushed &= sender.isFlushed();
        }

        for (ObjectConsumer objectConsumer : this.shuffledConsumers) {
            flushed &= objectConsumer.isFlushed();
        }

        return flushed;
    }

    private int processShufflers(ProducerInputStream<Object> chunk, int lastConsumedSize) throws Exception {
        if (this.calculationStrategies.length > 0) {
            int toIdx = Math.min(lastConsumedSize + this.chunkSize, chunk.size());
            int consumedSize = 0;

            for (int i = lastConsumedSize; i < toIdx; i++) {
                Object object = chunk.get(i);

                consumedSize++;
                processCalculationStrategies(object);
            }

            flush();
            return lastConsumedSize + consumedSize;
        } else {
            writeToNonPartitionedLocals(chunk);

            if (!this.receiver) {
                sendToNonPartitionedRemotes(chunk);
            }

            flush();
            return chunk.size();
        }
    }

    private void flush() {
        for (ObjectConsumer objectConsumer : this.shuffledConsumers) {
            objectConsumer.flush();
        }

        if (!this.receiver) {
            for (DataWriter sender : this.sendersArray) {
                sender.flush();
            }
        }
    }

    private boolean processCalculationStrategy(
            Object object,
            CalculationStrategy calculationStrategy,
            int objectPartitionId,
            CalculationStrategy objectCalculationStrategy,
            boolean markedNonPartitionedRemotes
    ) throws Exception {
        Map<Integer, List<ObjectConsumer>> cache = this.partitionedWriters.get(calculationStrategy);
        List<ObjectConsumer> writers = null;

        if (cache.size() > 0) {
            if (
                    (objectCalculationStrategy != null)
                            &&
                            (objectCalculationStrategy.equals(calculationStrategy))
                            &&
                            (objectPartitionId >= 0)
                    ) {
                writers = cache.get(objectPartitionId);
            } else {
                objectPartitionId = calculatePartitionId(object, calculationStrategy);
                writers = cache.get(objectPartitionId);
            }
        } else {
            //Send to another node
            objectPartitionId = calculatePartitionId(object, calculationStrategy);
        }

        Address address = this.nodeEngine.getPartitionService().getPartitionOwner(objectPartitionId);
        Address remoteJetAddress = containerContext.getApplicationContext().getHzToJetAddressMapping().get(address);
        DataWriter sender = this.senders.get(remoteJetAddress);

        if (sender != null) {
            markConsumer(sender);

            if (!markedNonPartitionedRemotes) {
                for (Address remoteAddress : this.nonPartitionedAddresses) {
                    if (!remoteAddress.equals(address)) {
                        markConsumer(this.senders.get(remoteAddress));
                    }
                }
            }
        } else if (!JetUtil.isEmpty(writers)) {
            //Write to partitioned locals
            for (int ir = 0; ir < writers.size(); ir++) {
                markConsumer(writers.get(ir));
            }

            if (!markedNonPartitionedRemotes) {
                markNonPartitionedRemotes();
                markedNonPartitionedRemotes = true;
            }
        }

        return markedNonPartitionedRemotes;
    }

    private int calculatePartitionId(Object object, CalculationStrategy calculationStrategy) {
        return HashUtil.hashToIndex(calculationStrategy.hash(object),
                this.nodeEngine.getPartitionService().getPartitionCount());
    }

    private void processCalculationStrategies(Object object) throws Exception {
        CalculationStrategy objectCalculationStrategy = null;
        int objectPartitionId = -1;

        if (object instanceof CalculationStrategyAware) {
            CalculationStrategyAware calculationStrategyAware = ((CalculationStrategyAware) object);
            objectCalculationStrategy = calculationStrategyAware.getCalculationStrategy();
        }

        if (object instanceof PartitionIdAware) {
            objectPartitionId = ((PartitionIdAware) object).getPartitionId();
        }

        writeToNonPartitionedLocals(object);

        Arrays.fill(this.markers, null);

        boolean markedNonPartitionedRemotes = false;

        for (CalculationStrategy calculationStrategy : this.calculationStrategies) {
            markedNonPartitionedRemotes = processCalculationStrategy(
                    object,
                    calculationStrategy,
                    objectPartitionId,
                    objectCalculationStrategy,
                    markedNonPartitionedRemotes
            );
        }

        sendToMarked(object);
    }

    private void sendToMarked(Object object) throws Exception {
        for (ObjectConsumer marker : this.markers) {
            if (marker != null) {
                marker.consumeObject(object);
            }
        }
    }

    private void markConsumer(ObjectConsumer consumer) {
        int position = this.markersCache.get(consumer);
        this.markers[position] = consumer;
    }

    private void writeToNonPartitionedLocals(ProducerInputStream<Object> chunk) throws Exception {
        for (ObjectConsumer nonPartitionedWriter : this.nonPartitionedWriters) {
            nonPartitionedWriter.consumeChunk(chunk);
        }
    }

    private void sendToNonPartitionedRemotes(ProducerInputStream<Object> chunk) throws Exception {
        for (Address remoteAddress : this.nonPartitionedAddresses) {
            this.senders.get(remoteAddress).consumeChunk(chunk);
        }
    }

    private void writeToNonPartitionedLocals(Object object) throws Exception {
        for (ObjectConsumer nonPartitionedWriter : this.nonPartitionedWriters) {
            nonPartitionedWriter.consumeObject(object);
        }
    }

    private void markNonPartitionedRemotes() throws Exception {
        for (Address remoteAddress : this.nonPartitionedAddresses) {
            markConsumer(this.senders.get(remoteAddress));
        }
    }
}
