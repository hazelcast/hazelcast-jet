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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class CoLocatedClustersElasticsearchSourcesTest extends CommonElasticsearchSourcesTest {

    @Override
    protected SupplierEx<RestHighLevelClient> elasticClientSupplier() {
        return () -> new RestHighLevelClient(RestClient.builder(
                new HttpHost("172.21.0.2", 9200)
        ));
    }

    @Override
    protected JetInstance createJetInstance() {
        ClientConfig config = new JetClientConfig().setNetworkConfig(new ClientNetworkConfig().addAddress("172.21.0.2"));
        return Jet.newJetClient(config);
    }

    @Test
    public void shouldReadFromMultipleShardsUsingCoLocatedReading() throws IOException {
        initShardedIndex("my-index");

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(elasticClientSupplier())
                .searchRequestSupplier(() -> new SearchRequest("my-index"))
                .mapHitFn(SearchHit::getSourceAsString)
                .coLocatedReading(true)
                .build();

        p.readFrom(source)
         .writeTo(Sinks.list(results));

        submitJob(p);
        assertThat(results).hasSize(BATCH_SIZE);
    }
}
