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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.impl.util.Util;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.function.Supplier;

public final class ElasticSupport {

    // Elastic container takes long time to start up, reusing the container for speedup
    public static final Supplier<ElasticsearchContainer> elastic = Util.memoize(() -> {
        ElasticsearchContainer elastic = new ElasticsearchContainer(ElasticsearchBaseTest.ELASTICSEARCH_IMAGE);
        elastic.start();
        Runtime.getRuntime().addShutdownHook(new Thread(elastic::stop));
        return elastic;
    });

    private ElasticSupport() {
    }

    public static SupplierEx<RestHighLevelClient> elasticClientSupplier() {
        String address = elastic.get().getHttpHostAddress();
        return () -> new RestHighLevelClient(
                RestClient.builder(HttpHost.create(address))
        );
    }
}
