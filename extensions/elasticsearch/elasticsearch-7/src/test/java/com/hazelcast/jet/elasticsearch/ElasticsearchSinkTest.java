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

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import org.junit.Test;

import java.io.IOException;

import static com.hazelcast.jet.elasticsearch.ElasticsearchSinks.elasticsearch;

public class ElasticsearchSinkTest extends ElasticsearchBaseTest {

    @Test
    public void test_elasticsearchSink() throws IOException {
        String containerAddress = container.getHttpHostAddress();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(userList))
         .writeTo(ElasticsearchSinks.elasticsearch(indexName, () -> createClient(containerAddress), indexFn(indexName)));

        jet.newJob(p).join();

        assertIndexes();
    }
}
