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

package com.hazelcast.jet.impl.dag;

import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.jet.spi.strategy.ProcessingStrategy;
import com.hazelcast.jet.spi.strategy.DataTransferringStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;

public class EdgeImpl implements Edge {
    private Vertex to;
    private String name;
    private Vertex from;
    private boolean shuffled;

    private HashingStrategy hashingStrategy;
    private ShufflingStrategy shufflingStrategy;
    private ProcessingStrategy processingStrategy;
    private PartitioningStrategy partitioningStrategy;
    private DataTransferringStrategy dataTransferringStrategy;

    public EdgeImpl(String name,
                    Vertex from,
                    Vertex to) {
        this(name, from, to, false);
    }

    public EdgeImpl(String name,
                    Vertex from,
                    Vertex to,
                    boolean shuffled) {
        this(name, from, to, shuffled, null, null, null, null, null);
    }

    public EdgeImpl(String name,
                    Vertex from,
                    Vertex to,
                    boolean shuffled,
                    ShufflingStrategy shufflingStrategy,
                    ProcessingStrategy processingStrategy,
                    PartitioningStrategy partitioningStrategy,
                    HashingStrategy hashingStrategy,
                    DataTransferringStrategy dataTransferringStrategy) {
        this.to = to;
        this.name = name;
        this.from = from;
        this.shuffled = shuffled;
        this.shufflingStrategy = shufflingStrategy;
        this.hashingStrategy = nvl(hashingStrategy, DefaultHashingStrategy.INSTANCE);
        this.partitioningStrategy = nvl(partitioningStrategy, StringPartitioningStrategy.INSTANCE);
        this.dataTransferringStrategy = nvl(dataTransferringStrategy, ByReferenceDataTransferringStrategy.INSTANCE);
        this.processingStrategy = nvl(processingStrategy, ProcessingStrategy.ROUND_ROBIN);
    }

    private <T> T nvl(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    @Override
    public Vertex getOutputVertex() {
        return this.to;
    }

    @Override
    public boolean isShuffled() {
        return this.shuffled;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Vertex getInputVertex() {
        return this.from;
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.shufflingStrategy;
    }

    @Override
    public ProcessingStrategy getProcessingStrategy() {
        return this.processingStrategy;
    }

    @Override
    public PartitioningStrategy getPartitioningStrategy() {
        return this.partitioningStrategy;
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return this.hashingStrategy;
    }

    @Override
    public DataTransferringStrategy getDataTransferringStrategy() {
        return this.dataTransferringStrategy;
    }

    public static class EdgeBuilder {
        private final EdgeImpl edge;
        private boolean build;

        public EdgeBuilder(String name,
                           Vertex from,
                           Vertex to) {
            this.edge = new EdgeImpl(name, from, to);
        }

        public EdgeBuilder shuffling(boolean shuffled) {
            this.edge.shuffled = shuffled;
            return this;
        }

        public EdgeBuilder shufflingStrategy(ShufflingStrategy shufflingStrategy) {
            this.edge.shufflingStrategy = shufflingStrategy;
            return this;
        }

        public EdgeBuilder processingStrategy(ProcessingStrategy processingStrategy) {
            this.edge.processingStrategy = processingStrategy;
            return this;
        }

        public EdgeBuilder partitioningStrategy(PartitioningStrategy partitioningStrategy) {
            this.edge.partitioningStrategy = partitioningStrategy;
            return this;
        }

        public EdgeBuilder hashingStrategy(HashingStrategy hashingStrategy) {
            this.edge.hashingStrategy = hashingStrategy;
            return this;
        }

        public EdgeBuilder dataTransferringStrategy(DataTransferringStrategy dataTransferringStrategy) {
            this.edge.dataTransferringStrategy = dataTransferringStrategy;
            return this;
        }

        public Edge build() {
            if (this.build) {
                throw new IllegalStateException("Edge has been already built");
            }

            this.build = true;
            return this.edge;
        }
    }
}
