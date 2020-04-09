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

import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nonnull;

import static org.apache.http.auth.AuthScope.ANY;

public final class ElasticClients {

    private static final int DEFAULT_PORT = 9200;

    private ElasticClients() {
    }

    @Nonnull
    public static RestHighLevelClient client() {
        return client("localhost", DEFAULT_PORT);
    }

    @Nonnull
    public static RestHighLevelClient client(@Nonnull String hostname, int port) {
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
    @Nonnull
    public static RestHighLevelClient client(
            @Nonnull String username,
            @Nonnull String password,
            @Nonnull String hostname,
            int port
    ) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(ANY, new UsernamePasswordCredentials(username, password));
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostname, port))
                          .setHttpClientConfigCallback(httpClientBuilder ->
                                  httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
        );
    }
}
