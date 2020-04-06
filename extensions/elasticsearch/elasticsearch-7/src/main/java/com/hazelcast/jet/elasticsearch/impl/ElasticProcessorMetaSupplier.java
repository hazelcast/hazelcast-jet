/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.elasticsearch.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.elasticsearch.ElasticsearchSourceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class ElasticProcessorMetaSupplier<T> implements ProcessorMetaSupplier {

    private static final long serialVersionUID = 1L;

    private transient ILogger logger;

    @Nonnull
    private final ElasticsearchSourceBuilder<T> builder;

    private Map<String, List<Shard>> assignedShards;
    private transient Address ownerAddress;

    public ElasticProcessorMetaSupplier(@Nonnull ElasticsearchSourceBuilder<T> builder) {
        this.builder = builder;
    }

    @Nonnull
    @Override
    public Map<String, String> getTags() {
        return emptyMap();
    }

    @Override
    public int preferredLocalParallelism() {
        if (builder.coLocatedReading() || builder.slicing()) {
            return builder.preferredLocalParallelism();
        } else {
            return 1;
        }
    }


    @Override
    public void init(@Nonnull Context context) throws Exception {
        logger = context.logger();

        ElasticCatClient catClient = new ElasticCatClient(builder.clientSupplier().get().getLowLevelClient());
        List<Shard> shards = catClient.shards(builder.searchRequestSupplier().get().indices());

        if (builder.coLocatedReading()) {
            List<String> addresses = context
                    .jetInstance().getCluster().getMembers().stream()
                    .map(m -> uncheckCall((() -> m.getAddress().getInetAddress().getHostAddress())))
                    .collect(toList());
            this.assignedShards = assignShards(shards, addresses);
        } else {
            String key = StringPartitioningStrategy.getPartitionKey(String.valueOf(context.jobId()));
            ownerAddress = context.jetInstance().getHazelcastInstance().getPartitionService()
                                  .getPartition(key).getOwner().getAddress();
        }

    }

    static Map<String, List<Shard>> assignShards(List<Shard> shards, List<String> addresses) {
        Map<String, List<Shard>> nodeCandidates = shards.stream()
                                                        .collect(groupingBy(Shard::getIp));
        Map<String, List<Shard>> nodeAssigned = new HashMap<>();

        if (!addresses.containsAll(nodeCandidates.keySet())) {
            throw new JetException("Shard locations are not equal to Jet nodes locations, " +
                    "shards=" + nodeCandidates.keySet() +
                    ", Jet nodes=" + addresses);
        }

        int uniqueShards = (int) shards.stream().map(Shard::indexShard).distinct().count();
        Set<String> assignedShards = new HashSet<>();

        int iterations = (uniqueShards + addresses.size() - 1) / addresses.size(); // Same as Math.ceil for float div
        for (int i = 0; i < iterations; i++) {
            for (String address : addresses) {
                List<Shard> thisNodeCandidates = nodeCandidates.getOrDefault(address, emptyList());
                if (thisNodeCandidates.isEmpty()) {
                    continue;
                }
                Shard shard = thisNodeCandidates.remove(0);

                List<Shard> nodeShards = nodeAssigned.computeIfAbsent(address, (key) -> new ArrayList<>());
                nodeShards.add(shard);

                nodeCandidates.values().forEach(candidates ->
                        candidates.removeIf(next -> next.indexShard().equals(shard.indexShard())));

                assignedShards.add(shard.indexShard());
            }
        }
        if (assignedShards.size() != uniqueShards) {
            throw new JetException("Not all shards have been assigned");
        }
        return nodeAssigned;
    }

    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        if (builder.slicing()) {
            return address -> new ElasticProcessorSupplier<>(builder);
        }
        if (builder.coLocatedReading()) {
            return address -> {
                String ipAddress = uncheckCall(() -> address.getInetAddress().getHostAddress());
                List<Shard> shards = assignedShards.get(ipAddress);
                return new ElasticProcessorSupplier<>(builder, shards);
            };
        } else {
            return address -> address.equals(ownerAddress) ? new ElasticProcessorSupplier<>(builder)
                    : count -> nCopies(count, Processors.noopP().get());
        }
    }

}
