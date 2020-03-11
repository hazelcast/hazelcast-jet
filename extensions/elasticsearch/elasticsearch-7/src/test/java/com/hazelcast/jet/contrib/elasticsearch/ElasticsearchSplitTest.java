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

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.collection.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.junit.Assert.assertEquals;

public class ElasticsearchSplitTest {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchSplitTest.class);


    @Test
    public void testSplit() {


        Pipeline p = Pipeline.create();

        /*p.readFrom(ElasticsearchSources.elasticsearch(
                () -> createClient("172.21.0.2:9200"),
                () -> {
                    SearchRequest searchRequest = new SearchRequest("my-another-index");
                    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//                    searchSourceBuilder.query(termQuery("age", 8));
                    searchRequest.source(searchSourceBuilder);
                    return searchRequest;
                }, SearchHit::getSourceAsString
        ))*/
//         .writeTo(Sinks.logger());

        JetInstance jet = Jet.newJetClient(new JetClientConfig().setNetworkConfig(new ClientNetworkConfig().addAddress("172.21.0.2")));

        JobConfig config = new JobConfig().addClass(ElasticsearchSplitTest.class)
                                          .addClass(ElasticsearchBaseTest.class);


        jet.newJob(p, config).join();

        IList<Object> sink = jet.getList("sink");
        assertEquals(1, sink.size());

    }

    static RestHighLevelClient createClient(String containerAddress) {
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(containerAddress)));
    }

    @Test
    public void name() throws IOException {


        RestClientBuilder builder = RestClient.builder(
                HttpHost.create("http://172.21.0.2:9200")
//                HttpHost.create("http://172.22.0.3:9200"),
//                HttpHost.create("http://172.22.0.4:9200")
        );

        builder.setNodeSelector(nodes -> {

            for (Node node : nodes) {
                System.out.println("node: " + node.getHost());
            }

        })
        ;
        RestHighLevelClient client = new RestHighLevelClient(builder);

/*
        Request r = new Request("GET", "/_cat/shards");
        r.addParameter("format", "json");
        Response res = client.getLowLevelClient().performRequest(r);
*/

//        String shards = IOUtils.toString(res.getEntity().getContent(), UTF_8);
//        System.out.println(shards);
//        JsonValue value = Json.parse(new InputStreamReader(res.getEntity().getContent()));
//        for (JsonValue shard : value.asArray()) {
//            shard.asObject().get("");
//        }
/*
        client.indices().delete(new DeleteIndexRequest("my-index"), RequestOptions.DEFAULT);
        log.info("Dropped index");
*/


        /*
        CreateIndexRequest index = new CreateIndexRequest("my-another-index");
        index.settings(Settings.builder()
                               .put("index.number_of_shards", 3)
                               .put("index.number_of_replicas", 1)
        );

        CreateIndexResponse response = client.indices().create(index, RequestOptions.DEFAULT);
        System.out.println(response);
        log.info("Created index");
        */

        /*

        for (int i = 0; i < 100; i++) {
            IndexRequest request = new IndexRequest("my-another-index");
            request.id("id" + i);

            Map<String, Object> document = new HashMap<>();
            document.put("title", "title " + i);
            document.put("number", i);
            request.source(document);
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            System.out.println(indexResponse);
        }
         */

/*        Map<Integer, List<String>> shards = IntStream.range(0, 100)
                                                      .mapToObj(i -> "id" + i)
                                                      .collect(groupingBy(routing -> Math.abs(Murmur3HashFunction.hash(routing)) % 3));

*/

        SearchRequest sr = new SearchRequest("my-z-index*");
        sr.source(new SearchSourceBuilder().size(1000));
        sr.scroll(TimeValue.timeValueMinutes(1L));
//        sr.preference("_shards:2|_only_local");

        SearchResponse search = client.search(sr, RequestOptions.DEFAULT);
        String scrollId = search.getScrollId();

        SearchHits hits = search.getHits();
        long processedHits = 0;
        List<String> ids = new ArrayList<>();
        while (true) {
            System.out.println("scroll id " + scrollId);
            System.out.println("next batch");

            for (SearchHit hit : hits) {
                System.out.println(hit.getId());
                ids.add(hit.getId());
                processedHits++;
            }

            if (hits.getTotalHits().value <= processedHits) {
                break;
            }

            SearchResponse scroll = client.scroll(new SearchScrollRequest(scrollId).scroll(TimeValue.timeValueMinutes(1L)), RequestOptions.DEFAULT);
            hits = scroll.getHits();
            scrollId = scroll.getScrollId();
        }


        ids.sort(String::compareTo);

        System.out.println(ids);
    }

    private void printShard(Map<Integer, List<String>> shards, int key) {
        List<String> expected = shards.get(key);
        expected.sort(String::compareTo);
        System.out.println(expected);
    }
}
