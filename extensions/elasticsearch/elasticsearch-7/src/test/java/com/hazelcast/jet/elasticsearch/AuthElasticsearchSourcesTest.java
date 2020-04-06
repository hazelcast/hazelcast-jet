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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.After;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.function.Supplier;

import static com.hazelcast.jet.elasticsearch.ElasticsearchBaseTest.ELASTICSEARCH_IMAGE;
import static com.hazelcast.jet.elasticsearch.ElasticsearchSources.client;
import static com.hazelcast.jet.elasticsearch.ElasticsearchSources.elasticsearch;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AuthElasticsearchSourcesTest extends BaseElasticsearchTest {

    /**
     * Using elastic container configured with security enabled
     */
    public static Supplier<ElasticsearchContainer> elastic = Util.memoize(() -> {
        ElasticsearchContainer elastic = new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                .withEnv("ELASTIC_USERNAME", "elastic")
                .withEnv("ELASTIC_PASSWORD", "SuperSecret")
                .withEnv("xpack.security.enabled", "true");

        elastic.start();
        Runtime.getRuntime().addShutdownHook(new Thread(elastic::stop));
        return elastic;
    });
    public static final int PORT = 9200;

    private final JetTestInstanceFactory factory = new JetTestInstanceFactory();

    @After
    public void afterClass() throws Exception {
        factory.terminateAll();
    }

    @Override
    protected SupplierEx<RestHighLevelClient> elasticClientSupplier() {
        ElasticsearchContainer container = elastic.get();
        String containerIp = container.getContainerIpAddress();
        Integer port = container.getMappedPort(PORT);

        return () -> client("elastic", "SuperSecret", containerIp, port);
    }

    @Override
    protected JetInstance createJetInstance() {
        return factory.newMember(new JetConfig());
    }

    @Test
    public void testAuthenticatedClient() {
        indexDocument("my-index", ImmutableMap.of("name", "Frantisek"));

        Pipeline p = Pipeline.create();
        p.readFrom(elasticsearch(elasticClientSupplier()))
         .writeTo(Sinks.list(results));

        submitJob(p);
    }

    @Test
    public void shouldFailWithAuthenticationException() {
        ElasticsearchContainer container = elastic.get();
        String containerIp = container.getContainerIpAddress();
        Integer port = container.getMappedPort(PORT);

        Pipeline p = Pipeline.create();
        p.readFrom(elasticsearch(() -> client("elastic", "WrongPassword", containerIp, port)))
         .writeTo(Sinks.list(results));

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(ResponseException.class)
                .hasStackTraceContaining("failed to authenticate user [elastic]");
    }

}
