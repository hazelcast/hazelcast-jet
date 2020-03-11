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

import com.hazelcast.jet.pipeline.BatchSource;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ElasticsearchSourceBuilderTest {

    @Test
    public void sourceHasCorrectName() {
        BatchSource<Object> source = new ElasticsearchSourceBuilder<>().build();
        assertThat(source.name()).isEqualTo("elastic");

        BatchSource<Object> namedSource = new ElasticsearchSourceBuilder<>()
                .name("CustomName")
                .build();
        assertThat(namedSource.name()).isEqualTo("CustomName");
    }

}