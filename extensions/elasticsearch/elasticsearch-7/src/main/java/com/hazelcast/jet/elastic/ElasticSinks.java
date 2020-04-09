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

package com.hazelcast.jet.elastic;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nonnull;

/**
 * Provides factory methods for Elasticsearch sinks.
 * Alternatively you can use {@link ElasticSinkBuilder}
 *
 * @since 4.1
 */
public final class ElasticSinks {

    private ElasticSinks() {
    }

    /**
     * Creates an Elasticsearch sink, uses a local instance of Elasticsearch
     *
     * @param mapItemFn function that maps items from a stream to an indexing request
     */
    @Nonnull
    public static <T> Sink<T> elastic(@Nonnull FunctionEx<? super T, ? extends DocWriteRequest<?>> mapItemFn) {
        return elastic(ElasticClients::client, mapItemFn);
    }

    /**
     * Creates an Elasticsearch sink, uses provided clientSupplier and mapItemFn
     *
     * @param clientSupplier client supplier
     * @param mapItemFn      function that maps items from a stream to an indexing request
     * @param <T>            type of incoming items
     */
    @Nonnull
    public static <T> Sink<T> elastic(
            @Nonnull SupplierEx<RestHighLevelClient> clientSupplier,
            @Nonnull FunctionEx<? super T, ? extends DocWriteRequest<?>> mapItemFn
    ) {
        return new ElasticSinkBuilder<T>()
                .clientSupplier(clientSupplier)
                .mapItemFn(mapItemFn)
                .build();
    }

    /**
     * Returns {@link ElasticSinkBuilder}
     *
     * @param <T> type of the items in the pipeline
     */
    @Nonnull
    public static <T> ElasticSinkBuilder<T> builder() {
        return new ElasticSinkBuilder<T>();
    }

}
