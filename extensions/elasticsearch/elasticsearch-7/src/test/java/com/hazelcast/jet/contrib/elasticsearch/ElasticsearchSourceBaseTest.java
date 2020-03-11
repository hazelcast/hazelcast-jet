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

import com.hazelcast.collection.IList;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;
import static org.elasticsearch.client.RequestOptions.DEFAULT;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of;

/**
 * Base class for Elasticsearch source tests
 * <p>
 * This class is to be extended for each type of environment to run on, e.g.
 * - simple 1 node Jet & Elastic instances
 * - co-located clusters of Jet and Elastic
 * - non co-located clusters of Jet and Elastic
 * <p>
 * Subclasses may add tests specific for particular type of environment.
 * <p>
 * RestHighLevelClient is used to create data in Elastic to isolate possible Source and Sink issues.
 */
public abstract class ElasticsearchSourceBaseTest {

    protected static final int BATCH_SIZE = 42;

    protected RestHighLevelClient elasticClient;
    protected JetInstance jet;
    protected IList<String> items;

    @Before
    public void setUpBase() throws Exception {
        if (elasticClient == null) {
            elasticClient = createElasticClient().get();
        }
        if (jet == null) {
            jet = createJetInstance();
        }
        cleanElasticData();
        items = jet.getList("result");
        items.clear();
    }

    protected abstract SupplierEx<RestHighLevelClient> createElasticClient();

    protected abstract JetInstance createJetInstance();

    @Test
    public void shouldReadEmptyIndex() throws IOException {
        // elasticClient.indices().create(new CreateIndexRequest("my-index"), DEFAULT);

        // TODO ideally we would just create the index but it gives "field _id not found" when there are no documents
        // in the index, not sure if it is an Elastic bug or wrong setup
        indexDocument("my-index", of("name", "Frantisek"));
        deleteDocuments();

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(createElasticClient())
                .searchRequestSupplier(() -> new SearchRequest("my-index"))
                .mapHitFn(SearchHit::getSourceAsString)
                .build();

        p.readFrom(source)
         .writeTo(Sinks.list(items));

        submitJob(p);

        assertThat(items).isEmpty();
    }

    @Test
    public void shouldReadSingleDocumentFromIndex() {
        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(createElasticClient())
                .searchRequestSupplier(() -> new SearchRequest("my-index"))
                .mapHitFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
         .writeTo(Sinks.list(items));

        submitJob(p);
        assertThat(items).containsExactly("Frantisek");
    }

    @Test
    public void shouldReadMultipleDocumentsFromIndexUsingScroll() throws IOException {
        indexBatchOfDocuments("my-index");

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(createElasticClient())
                .searchRequestSupplier(() -> {
                    SearchRequest sr = new SearchRequest("my-index");

                    sr.source().size(10) // needs to scroll 5 times
                      .query(matchAllQuery());
                    return sr;
                })
                .mapHitFn(SearchHit::getSourceAsString)
                .build();

        p.readFrom(source)
         .writeTo(Sinks.list(items));

        submitJob(p);
        assertThat(items).hasSize(BATCH_SIZE);
    }

    @Test
    public void shouldReadFromMultipleIndexes() {
        indexDocument("my-index-1", of("name", "Frantisek"));
        indexDocument("my-index-2", of("name", "Vladimir"));

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(createElasticClient())
                .searchRequestSupplier(() -> new SearchRequest("my-index-*"))
                .mapHitFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
         .writeTo(Sinks.list(items));

        submitJob(p);
        assertThat(items).containsOnlyOnce("Frantisek", "Vladimir");
    }

    @Test
    public void shouldNotReadFromIndexesNotSpecifiedInRequest() {
        indexDocument("my-index-1", of("name", "Frantisek"));
        indexDocument("my-index-2", of("name", "Vladimir"));

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(createElasticClient())
                .searchRequestSupplier(() -> new SearchRequest("my-index-1"))
                .mapHitFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
         .writeTo(Sinks.list(items));

        submitJob(p);
        assertThat(items).containsOnlyOnce("Frantisek");
    }

    @Test
    public void shouldReadOnlyMatchingDocuments() {
        indexDocument("my-index", of("name", "Frantisek"));
        indexDocument("my-index", of("name", "Vladimir"));

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(createElasticClient())
                .searchRequestSupplier(() -> new SearchRequest("my-index")
                        .source(new SearchSourceBuilder().query(QueryBuilders.matchQuery("name", "Frantisek"))))
                .mapHitFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
         .writeTo(Sinks.list(items));

        submitJob(p);
        assertThat(items).containsOnlyOnce("Frantisek");
    }

    @Test
    public void shouldReadFromMultipleShardsUsingSlices() throws IOException {
        initShardedIndex("my-index");

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(createElasticClient())
                .searchRequestSupplier(() -> new SearchRequest("my-index"))
                .mapHitFn(SearchHit::getSourceAsString)
                .slicing(true)
                .build();

        p.readFrom(source)
         .writeTo(Sinks.list(items));

        submitJob(p);
        assertThat(items).hasSize(BATCH_SIZE);
    }

    @Test
    public void shouldReadFromMultipleIndexesWithMultipleShardsUsingSlices() throws IOException {
        initShardedIndex("my-index-1");
        initShardedIndex("my-index-2");

        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(createElasticClient())
                .searchRequestSupplier(() -> new SearchRequest("my-index-*"))
                .mapHitFn(SearchHit::getSourceAsString)
                .slicing(true)
                .build();

        p.readFrom(source)
         .writeTo(Sinks.list(items));

        submitJob(p);
        assertThat(items).hasSize(2 * BATCH_SIZE);
    }

    protected void initShardedIndex(String index) throws IOException {
        CreateIndexRequest indexRequest = new CreateIndexRequest(index);
        indexRequest.settings(Settings.builder()
                                      .put("index.number_of_shards", 3)
                                      .put("index.number_of_replicas", 0)
        );

        elasticClient.indices().create(indexRequest, RequestOptions.DEFAULT);

        indexBatchOfDocuments(index);
    }

    protected void cleanElasticData() {
        try {
            deleteDocuments();

            elasticClient.indices().delete(new DeleteIndexRequest("*"), DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void deleteDocuments() throws IOException {
        DeleteByQueryRequest request = new DeleteByQueryRequest("*")
                .setQuery(matchAllQuery())
                .setRefresh(true);
        elasticClient.deleteByQuery(request, DEFAULT);
    }

    protected void indexBatchOfDocuments(String index) {
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            docs.add(of("title", "document " + i));
        }
        indexDocuments(index, docs);
    }

    protected List<String> indexDocument(String index, Map<String, Object> document) {
        return indexDocuments(index, newArrayList(document));
    }

    protected List<String> indexDocuments(String index, List<Map<String, Object>> documents) {
        BulkRequest request = new BulkRequest()
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        for (Map<String, Object> document : documents) {
            request.add(new IndexRequest(index)
                    .source(document));
        }

        try {
            BulkResponse response = elasticClient.bulk(request, RequestOptions.DEFAULT);
            return Arrays.stream(response.getItems())
                         .map(BulkItemResponse::getId)
                         .collect(Collectors.toList());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void submitJob(Pipeline p) {
        JobConfig config = new JobConfig();

        config.addClass(
                // TODO this class will be moved to integration test suite
                CoLocatedClustersElasticsearchSourceTest.class,
                ElasticsearchSourceBaseTest.class
        );

        jet.newJob(p, config).join();
    }

}
