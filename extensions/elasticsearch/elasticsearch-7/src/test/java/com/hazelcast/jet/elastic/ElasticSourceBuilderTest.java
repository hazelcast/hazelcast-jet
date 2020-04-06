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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.BatchSource;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ElasticSourceBuilderTest {

    @Test
    public void sourceHasCorrectName() {
        BatchSource<Object> source = builderWithRequiredParams()
                .build();
        assertThat(source.name()).isEqualTo("elastic");

        BatchSource<Object> namedSource = builderWithRequiredParams()
                .name("CustomName")
                .build();
        assertThat(namedSource.name()).isEqualTo("CustomName");
    }

    @NotNull
    private ElasticSourceBuilder<Object> builderWithRequiredParams() {
        return new ElasticSourceBuilder<>()
                .clientSupplier(() -> new RestHighLevelClient(RestClient.builder(new HttpHost("localhost"))))
                .searchRequestSupplier(SearchRequest::new)
                .mapHitFn(FunctionEx.identity());
    }

    @Test
    public void clientSupplierMustBeSet() {
        assertThatThrownBy(() -> new ElasticSourceBuilder<>()
                .searchRequestSupplier(SearchRequest::new)
                .mapHitFn(FunctionEx.identity())
                .build())
                .hasMessage("clientSupplier must be set");
    }

    @Test
    public void searchRequestSupplierMustBeSet() {
        assertThatThrownBy(() -> new ElasticSourceBuilder<>()
                .clientSupplier(() -> new RestHighLevelClient(RestClient.builder(new HttpHost("localhost"))))
                .mapHitFn(FunctionEx.identity())
                .build())
                .hasMessage("searchRequestSupplier must be set");
    }

    @Test
    public void mapHitFnSupplierMustBeSet() {
        assertThatThrownBy(() -> new ElasticSourceBuilder()
                .clientSupplier(() -> new RestHighLevelClient(RestClient.builder(new HttpHost("localhost"))))
                .searchRequestSupplier(SearchRequest::new)
                .build())
                .hasMessage("mapHitFn must be set");
    }

}
