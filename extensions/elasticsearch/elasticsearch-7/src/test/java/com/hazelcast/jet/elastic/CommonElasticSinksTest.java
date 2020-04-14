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

import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

public abstract class CommonElasticSinksTest extends BaseElasticTest {

    @Test
    public void shouldStoreDocument() throws Exception {
        Sink<TestItem> elasticSink = new ElasticSinkBuilder<TestItem>()
                .clientSupplier(elasticClientSupplier())
                .bulkRequestSupplier(() -> new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                .mapItemFn(item -> new IndexRequest("my-index").source(item.asMap()))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem("id", "Frantisek")))
         .writeTo(elasticSink);

        submitJob(p);

        assertSingleDocument();
    }

    @Test
    public void shouldStoreBatchOfDocuments() throws IOException {
        Sink<TestItem> elasticSink = new ElasticSinkBuilder<TestItem>()
                .clientSupplier(elasticClientSupplier())
                .mapItemFn(item -> new IndexRequest("my-index").source(item.asMap()))
                .build();

        int batchSize = 10_000;
        TestItem[] items = new TestItem[batchSize];
        for (int i = 0; i < batchSize; i++) {
            items[i] = new TestItem("id" + i, "name" + i);
        }
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(items))
         .writeTo(elasticSink);

        submitJob(p);
        refreshIndex();

        SearchResponse response = elasticClient.search(new SearchRequest("my-index"), DEFAULT);
        TotalHits totalHits = response.getHits().getTotalHits();
        assertThat(totalHits.value).isEqualTo(batchSize);
    }

    @Test
    public void whenCreateSinkUsingFactoryMethodThenShouldStoreDocument() throws Exception {
        Sink<TestItem> elasticSink = ElasticSinks.elastic(
                elasticClientSupplier(),
                item -> new IndexRequest("my-index").source(item.asMap())
        );

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(new TestItem("id", "Frantisek")))
         .writeTo(elasticSink);

        submitJob(p);
        refreshIndex();

        assertSingleDocument();
    }

    private void refreshIndex() throws IOException {
        // Need to refresh index because the default bulk request doesn't do it and we may not see the result
        elasticClient.indices().refresh(new RefreshRequest("my-index"), DEFAULT);
    }

    private void assertSingleDocument() throws IOException {
        SearchResponse response = elasticClient.search(new SearchRequest("my-index"), DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        assertThat(hits).hasSize(1);
        Map<String, Object> document = hits[0].getSourceAsMap();
        assertThat(document).contains(
                entry("id", "id"),
                entry("name", "Frantisek")
        );
    }

    static class TestItem implements Serializable {

        private final String id;
        private final String name;

        TestItem(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> asMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("id", id);
            map.put("name", name);
            return map;
        }

    }
}
