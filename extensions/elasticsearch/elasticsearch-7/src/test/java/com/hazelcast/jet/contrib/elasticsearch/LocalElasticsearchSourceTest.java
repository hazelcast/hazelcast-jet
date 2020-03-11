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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of;

/**
 * Test running single Jet member locally and Elastic in docker
 */
public class LocalElasticsearchSourceTest extends ElasticsearchSourceBaseTest {

    // Cluster startup takes >1s, reusing the cluster between tests
    private static Supplier<JetInstance> jet = Util.memoize(() -> {
        JetTestInstanceFactory factory = new JetTestInstanceFactory();
        JetInstance instance = factory.newMember(new JetConfig());
        Runtime.getRuntime().addShutdownHook(new Thread(factory::terminateAll));
        return instance;
    });

    protected SupplierEx<RestHighLevelClient> createElasticClient() {
        String address = ElasticSupport.elastic.get().getHttpHostAddress();
        return () -> new RestHighLevelClient(
                RestClient.builder(HttpHost.create(address))
        );
    }

    @Override
    protected JetInstance createJetInstance() {
        // This starts very quickly, no need to cache the instance
        return jet.get();
    }

    @Test
    public void shouldThrowExceptionForCoLocatedReading() {
        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(createElasticClient())
                .searchRequestSupplier(() -> new SearchRequest("my-index"))
                .mapHitFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .coLocatedReading(true)
                .build();

        p.readFrom(source)
         .writeTo(Sinks.logger());

        assertThatThrownBy(() -> super.jet.newJob(p).join())
                .hasCauseInstanceOf(JetException.class)
                .hasMessageContaining("Shard locations are not equal to Jet nodes locations");
    }
}
