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
import com.hazelcast.jet.pipeline.BatchSource;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nonnull;

/**
 * Provides factory methods for Elasticsearch sources.
 * Alternatively you can use {@link ElasticSourceBuilder}
 *
 * @since 4.1
 */
public final class ElasticSources {

    private ElasticSources() {
    }

    /**
     * Creates a source which queries local instance of Elasticsearch for all documents
     * <p>
     * Useful for quick prototyping. See other methods {@link #elastic(SupplierEx, SupplierEx, FunctionEx)}
     * and {@link #builder()}
     */
    @Nonnull
    public static BatchSource<String> elastic() {
        return elastic(ElasticClients::client);
    }

    /**
     * Creates a source which queries Elasticsearch using client obtained from {@link RestHighLevelClient} supplier.
     * Queries all indexes for all documents.
     * Uses {@link SearchHit#getSourceAsString()} as mapping function
     */
    @Nonnull
    public static BatchSource<String> elastic(@Nonnull SupplierEx<RestHighLevelClient> clientSupplier) {
        return elastic(clientSupplier, SearchHit::getSourceAsString);
    }

    /**
     * Creates a source which queries local instance of Elasticsearch for all documents
     * Uses {@link SearchHit#getSourceAsString()} as mapping function
     */
    @Nonnull
    public static <T> BatchSource<T> elastic(@Nonnull FunctionEx<? super SearchHit, T> mapHitFn) {
        return elastic(ElasticClients::client, mapHitFn);
    }

    /**
     * Creates a source which queries Elasticsearch using client obtained from {@link RestHighLevelClient} supplier.
     * Uses provided mapHitFn to map results.
     * Queries all indexes for all documents.
     *
     * @param clientSupplier RestHighLevelClient supplier
     * @param mapHitFn       supplier of a function mapping the result from SearchHit to a result type
     * @param <T>            result type returned by the map function
     */
    @Nonnull
    public static <T> BatchSource<T> elastic(
            @Nonnull SupplierEx<RestHighLevelClient> clientSupplier,
            @Nonnull FunctionEx<? super SearchHit, T> mapHitFn) {
        return elastic(clientSupplier, SearchRequest::new, mapHitFn);
    }

    /**
     * Creates a source which queries Elasticsearch using client obtained from {@link RestHighLevelClient} supplier.
     *
     * @param clientSupplier        RestHighLevelClient supplier
     * @param searchRequestSupplier supplier of a SearchRequest used to query for documents
     * @param mapHitFn              supplier of a function mapping the result from SearchHit to a target type
     * @param <T>                   result type returned by the map function
     */
    @Nonnull
    public static <T> BatchSource<T> elastic(
            @Nonnull SupplierEx<RestHighLevelClient> clientSupplier,
            @Nonnull SupplierEx<SearchRequest> searchRequestSupplier,
            @Nonnull FunctionEx<? super SearchHit, T> mapHitFn
    ) {
        return ElasticSources.<T>builder()
                .clientSupplier(clientSupplier)
                .searchRequestSupplier(searchRequestSupplier)
                .mapHitFn(mapHitFn)
                .build();
    }

    /**
     * Returns {@link ElasticSourceBuilder}
     *
     * @param <T> result type returned by the map function
     */
    @Nonnull
    public static <T> ElasticSourceBuilder<T> builder() {
        return new ElasticSourceBuilder<>();
    }

}
