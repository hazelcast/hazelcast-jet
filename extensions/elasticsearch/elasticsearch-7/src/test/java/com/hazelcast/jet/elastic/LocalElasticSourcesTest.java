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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.elasticsearch.action.search.SearchRequest;
import org.junit.After;
import org.junit.Test;

import static com.google.common.collect.ImmutableMap.of;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test running single Jet member locally and Elastic in docker
 */
public class LocalElasticSourcesTest extends CommonElasticSourcesTest {

    private JetTestInstanceFactory factory = new JetTestInstanceFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Override
    protected JetInstance createJetInstance() {
        // This starts very quickly, no need to cache the instance
        return factory.newMember(new JetConfig());
    }

    @Test
    public void shouldThrowExceptionForCoLocatedReading() {
        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<String>()
                .clientSupplier(elasticClientSupplier())
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
