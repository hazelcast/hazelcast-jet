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

package com.hazelcast.jet.elasticsearch;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nonnull;

import static org.apache.http.auth.AuthScope.ANY;

/**
 * Provides factory methods for Elasticsearch sinks.
 * Alternatively you can use {@link ElasticsearchSinkBuilder}
 *
 * @since 4.1
 */
public final class ElasticsearchSinks {

    private static final int PORT = 9200;

    private ElasticsearchSinks() {
    }

    /**
     * Creates an Elasticsearch sink, uses a local instance of Elasticsearch
     *
     * @param mapItemFn function that maps items from a stream to an indexing request
     */
    public static <T> Sink<T> elasticsearch(@Nonnull FunctionEx<? super T, ? extends DocWriteRequest<?>> mapItemFn) {
        return elasticsearch(() -> client("localhost", PORT), mapItemFn);
    }

    /**
     * Creates an Elasticsearch sink, uses provided clientSupplier and mapItemFn
     *
     * @param clientSupplier client supplier
     * @param mapItemFn      function that maps items from a stream to an indexing request
     * @param <T>            type of incoming items
     */
    public static <T> Sink<T> elasticsearch(
            @Nonnull SupplierEx<RestHighLevelClient> clientSupplier,
            @Nonnull FunctionEx<? super T, ? extends DocWriteRequest<?>> mapItemFn
    ) {
        return new ElasticsearchSinkBuilder<T>()
                .clientSupplier(clientSupplier)
                .mapItemFn(mapItemFn)
                .build();
    }

    /**
     * Returns {@link ElasticsearchSinkBuilder}
     *
     * @param <T> type of the items in the pipeline
     */
    public static <T> ElasticsearchSinkBuilder<T> builder() {
        return new ElasticsearchSinkBuilder<T>();
    }

    static RestHighLevelClient client(String hostname, int port) {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostname, port))
        );
    }

    /**
     * Convenience method to create {@link RestHighLevelClient} with basic authentication and given hostname and port
     * <p>
     * Usage:
     * <pre>
     *   BatchSource<SearchHit> source = elasticsearch(() -> client("user", "password", "host", 9200));
     * </pre>
     */
    public static RestHighLevelClient client(String username, String password, String hostname, int port) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(ANY, new UsernamePasswordCredentials(username, password));
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostname, port))
                          .setHttpClientConfigCallback(httpClientBuilder ->
                                  httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
        );
    }


}
