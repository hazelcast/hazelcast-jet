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
 * @since 4.2
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
    public static BatchSource<String> elastic(@Nonnull SupplierEx<RestHighLevelClient> clientFn) {
        return elastic(clientFn, SearchHit::getSourceAsString);
    }

    /**
     * Creates a source which queries local instance of Elasticsearch for all documents
     * Uses {@link SearchHit#getSourceAsString()} as mapping function
     */
    @Nonnull
    public static <T> BatchSource<T> elastic(@Nonnull FunctionEx<? super SearchHit, T> mapToItemFn) {
        return elastic(ElasticClients::client, mapToItemFn);
    }

    /**
     * Creates a source which queries Elasticsearch using client obtained from {@link RestHighLevelClient} supplier.
     * Uses provided mapToItemFn to map results.
     * Queries all indexes for all documents.
     *
     * @param clientFn    RestHighLevelClient supplier
     * @param mapToItemFn supplier of a function mapping the result from SearchHit to a result type
     * @param <T>         result type returned by the map function
     */
    @Nonnull
    public static <T> BatchSource<T> elastic(
            @Nonnull SupplierEx<RestHighLevelClient> clientFn,
            @Nonnull FunctionEx<? super SearchHit, T> mapToItemFn) {
        return elastic(clientFn, SearchRequest::new, mapToItemFn);
    }

    /**
     * Creates a source which queries Elasticsearch using client obtained from {@link RestHighLevelClient} supplier.
     *
     * @param clientFn        RestHighLevelClient supplier
     * @param searchRequestFn supplier function of a SearchRequest used to query for documents
     * @param mapToItemFn     supplier of a function mapping the result from SearchHit to a target type
     * @param <T>             result type returned by the map function
     */
    @Nonnull
    public static <T> BatchSource<T> elastic(
            @Nonnull SupplierEx<RestHighLevelClient> clientFn,
            @Nonnull SupplierEx<SearchRequest> searchRequestFn,
            @Nonnull FunctionEx<? super SearchHit, T> mapToItemFn
    ) {
        return ElasticSources.<T>builder()
                .clientFn(clientFn)
                .searchRequestFn(searchRequestFn)
                .mapToItemFn(mapToItemFn)
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
