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

package com.hazelcast.jet.contrib.elasticsearch;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Contains factory methods for Elasticsearch sources.
 */
public final class ElasticsearchSources {

    private static final String DEFAULT_SCROLL_TIMEOUT = "60s";

    private ElasticsearchSources() {
    }

    /**
     * Creates a source which queries objects using the specified Elasticsearch
     * client and the specified request supplier using scrolling method.
     *
     * @param name                  name of the source
     * @param clientSupplier        Elasticsearch REST client supplier
     * @param searchRequestSupplier search request supplier
     * @param scrollTimeout         scroll keep alive time
     * @param mapHitFn              maps search hits to output items
     * @param optionsFn             obtains {@link RequestOptions} for each request
     * @param destroyFn             called upon completion to release any resource
     * @param <T>                   type of items emitted downstream
     */
    public static <T> BatchSource<T> elasticsearch(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends RestHighLevelClient> clientSupplier,
            @Nonnull SupplierEx<SearchRequest> searchRequestSupplier,
            @Nonnull String scrollTimeout,
            @Nonnull FunctionEx<SearchHit, T> mapHitFn,
            @Nonnull FunctionEx<? super ActionRequest, RequestOptions> optionsFn,
            @Nonnull ConsumerEx<? super RestHighLevelClient> destroyFn
    ) {
        return SourceBuilder
                .batch(name, ctx -> new SearchContext<>(clientSupplier.get(), scrollTimeout,
                        mapHitFn, searchRequestSupplier.get(), optionsFn, destroyFn))
                .<T>fillBufferFn(SearchContext::fillBuffer)
                .destroyFn(SearchContext::close)
                .build();
    }

    /**
     * Convenience for {@link #elasticsearch(String, SupplierEx, SupplierEx,
     * String, FunctionEx, FunctionEx, ConsumerEx)}.
     * Uses {@link #DEFAULT_SCROLL_TIMEOUT} for scroll timeout and {@link
     * RequestOptions#DEFAULT}, emits string representation of items using
     * {@link SearchHit#getSourceAsString()} and closes the {@link
     * RestHighLevelClient} upon completion.
     */
    public static BatchSource<String> elasticsearch(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends RestHighLevelClient> clientSupplier,
            @Nonnull SupplierEx<SearchRequest> searchRequestSupplier
    ) {
        return elasticsearch(name, clientSupplier, searchRequestSupplier, DEFAULT_SCROLL_TIMEOUT,
                SearchHit::getSourceAsString, request -> RequestOptions.DEFAULT, RestHighLevelClient::close);
    }


    /**
     * Convenience for {@link #elasticsearch(String, SupplierEx, SupplierEx)}.
     * Rest client is configured with basic authentication.
     */
    public static BatchSource<String> elasticsearch(
            @Nonnull String name,
            @Nonnull String username,
            @Nullable String password,
            @Nonnull String hostname,
            int port,
            @Nonnull SupplierEx<SearchRequest> searchRequestSupplier
    ) {
        return elasticsearch(name, () -> ElasticsearchSinks.buildClient(username, password, hostname, port),
                searchRequestSupplier);
    }

    private static final class SearchContext<T> {

        private final RestHighLevelClient client;
        private final String scrollInterval;
        private final FunctionEx<SearchHit, T> mapHitFn;
        private final FunctionEx<? super ActionRequest, RequestOptions> optionsFn;
        private final ConsumerEx<? super RestHighLevelClient> destroyFn;

        private SearchResponse searchResponse;

        private SearchContext(RestHighLevelClient client, String scrollInterval,
                              FunctionEx<SearchHit, T> mapHitFn, SearchRequest searchRequest,
                              FunctionEx<? super ActionRequest, RequestOptions> optionsFn,
                              ConsumerEx<? super RestHighLevelClient> destroyFn
        ) throws IOException {
            this.client = client;
            this.scrollInterval = scrollInterval;
            this.mapHitFn = mapHitFn;
            this.optionsFn = optionsFn;
            this.destroyFn = destroyFn;

            searchRequest.scroll(scrollInterval);
            searchResponse = client.search(searchRequest, optionsFn.apply(searchRequest));
        }

        private void fillBuffer(SourceBuffer<T> buffer) throws IOException {
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (hits == null || hits.length == 0) {
                buffer.close();
                return;
            }
            for (SearchHit hit : hits) {
                T item = mapHitFn.apply(hit);
                if (item != null) {
                    buffer.add(item);
                }
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(searchResponse.getScrollId());
            scrollRequest.scroll(scrollInterval);
            searchResponse = client.scroll(scrollRequest, optionsFn.apply(scrollRequest));
        }

        private void clearScroll() throws IOException {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(searchResponse.getScrollId());
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        }

        private void close() throws IOException {
            clearScroll();
            destroyFn.accept(client);
        }
    }
}
