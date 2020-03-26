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

import com.hazelcast.jet.elasticsearch.ElasticsearchSourceBuilder;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.util.Util;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

class ElasticProcessorSupplier<R> implements ProcessorSupplier {

    private final ElasticsearchSourceBuilder<R> builder;

    private final List<Shard> shards;
    private Map<Integer, List<Shard>> shardsByProcessor;

    ElasticProcessorSupplier(@Nonnull ElasticsearchSourceBuilder<R> builder) {
        this.builder = builder;
        this.shards = null;
        assert !builder.coLocatedReading() : "Co-located reading is on but no shards given";
    }

    ElasticProcessorSupplier(@Nonnull ElasticsearchSourceBuilder<R> builder,
                             @Nonnull List<Shard> shards) {
        this.builder = builder;
        this.shards = shards;
    }


    @Override
    public void init(@Nonnull Context context) {
        if (builder.coLocatedReading()) {
            shardsByProcessor = Util.distributeObjects(context.localParallelism(), shards);
        }
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            if (builder.coLocatedReading()) {
                processors.add(new ElasticProcessor<>(builder, shardsByProcessor.get(i)));
            } else {
                processors.add(new ElasticProcessor<>(builder));
            }
        }
        return processors;
    }

}
