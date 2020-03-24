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

package com.hazelcast.jet.contrib.elasticsearch.impl;

import com.hazelcast.jet.JetException;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static com.hazelcast.jet.contrib.elasticsearch.impl.Shard.Prirep.p;
import static com.hazelcast.jet.contrib.elasticsearch.impl.Shard.Prirep.r;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.util.Lists.newArrayList;

public class ElasticProcessorMetaSupplierTest {

    @Test
    public void shouldAssignToSingleAddress() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, p, 10, "STARTED", "10.0.0.1", "node1"),
                new Shard("elastic-index", 1, p, 10, "STARTED", "10.0.0.1", "node2"),
                new Shard("elastic-index", 2, p, 10, "STARTED", "10.0.0.1", "node3")
        );

        List<String> addresses = newArrayList("10.0.0.1");
        Map<String, List<Shard>> assignment = ElasticProcessorMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).contains(
                entry("10.0.0.1", shards)
        );
    }

    @Test
    public void shouldAssignToEachAddress() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, p, 10, "STARTED", "10.0.0.1", "node1"),
                new Shard("elastic-index", 1, p, 10, "STARTED", "10.0.0.2", "node2"),
                new Shard("elastic-index", 2, p, 10, "STARTED", "10.0.0.3", "node3")
        );

        List<String> addresses = newArrayList("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<String, List<Shard>> assignment = ElasticProcessorMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).contains(
                entry("10.0.0.1", newArrayList(shards.get(0))),
                entry("10.0.0.2", newArrayList(shards.get(1))),
                entry("10.0.0.3", newArrayList(shards.get(2)))
        );
    }

    @Test
    public void shouldAssignOneReplicaOnly() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, p, 10, "STARTED", "10.0.0.1", "node1"),
                new Shard("elastic-index", 0, r, 10, "STARTED", "10.0.0.2", "node2"),
                new Shard("elastic-index", 0, r, 10, "STARTED", "10.0.0.3", "node3"),

                new Shard("elastic-index", 1, p, 10, "STARTED", "10.0.0.2", "node2"),
                new Shard("elastic-index", 1, r, 10, "STARTED", "10.0.0.3", "node3"),
                new Shard("elastic-index", 1, r, 10, "STARTED", "10.0.0.1", "node1"),

                new Shard("elastic-index", 2, p, 10, "STARTED", "10.0.0.3", "node3"),
                new Shard("elastic-index", 2, r, 10, "STARTED", "10.0.0.1", "node1"),
                new Shard("elastic-index", 2, r, 10, "STARTED", "10.0.0.2", "node2")
        );

        Collections.shuffle(shards, new Random(1L)); // random but stable shuffle

        List<String> addresses = newArrayList("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<String, List<Shard>> assignment = ElasticProcessorMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).containsKeys("10.0.0.1", "10.0.0.2", "10.0.0.3");

        assertThat(assignment.get("10.0.0.1")).hasSize(1);
        assertThat(assignment.get("10.0.0.2")).hasSize(1);
        assertThat(assignment.get("10.0.0.3")).hasSize(1);

        assertThat(assignment.get("10.0.0.1").get(0).getIp()).isEqualTo("10.0.0.1");
        assertThat(assignment.get("10.0.0.2").get(0).getIp()).isEqualTo("10.0.0.2");
        assertThat(assignment.get("10.0.0.3").get(0).getIp()).isEqualTo("10.0.0.3");

        List<String> indexShards = assignment.values()
                                             .stream()
                                             .flatMap(Collection::stream)
                                             .map(Shard::indexShard)
                                             .collect(Collectors.toList());

        assertThat(indexShards).containsOnly("elastic-index-0", "elastic-index-1", "elastic-index-2");
    }

    @Test
    public void noLocalCandidate() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, p, 10, "STARTED", "10.0.0.1", "node1"),
                new Shard("elastic-index", 1, p, 10, "STARTED", "10.0.0.2", "node2")
        );

        List<String> addresses = newArrayList("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<String, List<Shard>> assignment = ElasticProcessorMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).contains(
                entry("10.0.0.1", newArrayList(shards.get(0))),
                entry("10.0.0.2", newArrayList(shards.get(1)))
        );
    }

    @Test(expected = JetException.class)
    public void shouldNotAllowDisjunctClusters() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, p, 10, "STARTED", "10.0.0.1", "node1")
        );
        List<String> addresses = newArrayList("10.0.0.2");

        ElasticProcessorMetaSupplier.assignShards(shards, addresses);
    }
}
