/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.contrib.elasticsearch;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.junit.Assert.assertEquals;

public class ElasticsearchSourceTest extends ElasticsearchBaseTest {

    @Test
    public void test() throws IOException {
        String containerAddress = container.getHttpHostAddress();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(userList))
         .writeTo(ElasticsearchSinks.elasticsearch(indexName, () -> createClient(containerAddress),
                 () -> new BulkRequest().setRefreshPolicy(IMMEDIATE), indexFn(indexName), RestHighLevelClient::close));

        jet.newJob(p).join();

        assertIndexes();

        p = Pipeline.create();

        p.readFrom(ElasticsearchSources.elasticsearch("users", () -> createClient(containerAddress),
                () -> {
                    SearchRequest searchRequest = new SearchRequest("users");
                    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                    searchSourceBuilder.query(termQuery("age", 8));
                    searchRequest.source(searchSourceBuilder);
                    return searchRequest;
                }))
         .writeTo(Sinks.list("sink"));

        jet.newJob(p).join();

        IList<Object> sink = jet.getList("sink");
        assertEquals(1, sink.size());
    }

}
