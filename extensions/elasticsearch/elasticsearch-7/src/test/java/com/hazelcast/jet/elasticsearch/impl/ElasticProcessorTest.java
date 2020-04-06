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

package com.hazelcast.jet.elasticsearch.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.elasticsearch.ElasticsearchSourceBuilder;
import com.hazelcast.jet.elasticsearch.impl.Shard.Prirep;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RequestOptions.Builder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.util.Lists.newArrayList;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ElasticProcessorTest {

    public static final String HIT_SOURCE = "{\"name\": \"Frantisek\"}";
    public static final String HIT_SOURCE2 = "{\"name\": \"Vladimir\"}";
    public static final String SCROLL_ID = "random-scroll-id";
    public static final int OUTBOX_CAPACITY = 1000;

    private static final String KEEP_ALIVE = "42m";

    private ElasticProcessor<String> processor;
    private SerializableRestClient mockClient;
    private SearchResponse response;
    private TestOutbox outbox;

    @Before
    public void setUp() throws Exception {
        mockClient = SerializableRestClient.instanceHolder = mock(SerializableRestClient.class, RETURNS_DEEP_STUBS);
        // Mocks returning mocks is not generally recommended, but the setup of empty SearchResponse is even uglier
        // See org.elasticsearch.action.search.SearchResponse#empty
        response = mock(SearchResponse.class);
        when(response.getScrollId()).thenReturn(SCROLL_ID);
        when(mockClient.search(any(), any())).thenReturn(response);
    }

    private void createProcessor() throws Exception {
        createProcessor(request -> RequestOptions.DEFAULT, emptyList());
    }

    private void createProcessor(FunctionEx<ActionRequest, RequestOptions> optionsFn) throws Exception {
        createProcessor(optionsFn, emptyList());
    }

    private void createProcessor(List<Shard> shards) throws Exception {
        createProcessor(request -> RequestOptions.DEFAULT, shards);
    }

    private void createProcessor(FunctionEx<ActionRequest, RequestOptions> optionsFn, List<Shard> shards)
            throws Exception {

        RestHighLevelClient client = mockClient;
        ElasticsearchSourceBuilder<String> builder = new ElasticsearchSourceBuilder<String>()
                .clientSupplier(() -> client)
                .searchRequestSupplier(() -> new SearchRequest("*"))
                .optionsFn(optionsFn)
                .mapHitFn(SearchHit::getSourceAsString)
                .scrollKeepAlive(KEEP_ALIVE);

        if (!shards.isEmpty()) {
            builder.coLocatedReading(true);
        }

        // This constructor calls the client so it has to be called after specific mock setup in each test method
        // rather than in setUp()
        processor = new ElasticProcessor<>(builder, shards);
        outbox = new TestOutbox(OUTBOX_CAPACITY);
        processor.init(outbox, new TestProcessorContext());

    }

    @Test
    public void shouldUseScrollSearch() throws Exception {
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, new TotalHits(0, EQUAL_TO), Float.NaN));

        createProcessor();

        runProcessor();

        ArgumentCaptor<SearchRequest> captor = forClass(SearchRequest.class);
        verify(mockClient).search(captor.capture(), any());

        SearchRequest request = captor.getValue();
        assertThat(request.scroll().keepAlive().getStringRep()).isEqualTo(KEEP_ALIVE);
    }

    private List<String> runProcessor() {
        processor.complete();
        List<String> out = new ArrayList<>();
        outbox.drainQueueAndReset(0, out, false);
        return out;
    }

    @Test
    public void shouldUseOptionsFnForSearch() throws Exception {
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, new TotalHits(0, EQUAL_TO), Float.NaN));

        // get different instance than default
        createProcessor(request -> {
            Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.addHeader("TestHeader", "value");
            return builder.build();
        });

        runProcessor();

        ArgumentCaptor<RequestOptions> captor = forClass(RequestOptions.class);
        verify(mockClient).search(any(), captor.capture());

        RequestOptions capturedOptions = captor.getValue();
        assertThat(capturedOptions.getHeaders())
                .extracting(h -> tuple(h.getName(), h.getValue()))
                .containsExactly(tuple("TestHeader", "value"));
    }

    @Test
    public void shouldProduceSingleHit() throws Exception {
        SearchHit hit = new SearchHit(0, "id-0", new Text("ignored"), emptyMap());
        hit.sourceRef(new BytesArray(HIT_SOURCE));
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{hit}, new TotalHits(0, EQUAL_TO), Float.NaN));

        SearchResponse response2 = mock(SearchResponse.class);
        when(response2.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, new TotalHits(3, EQUAL_TO), Float.NaN));
        when(mockClient.scroll(any(), any())).thenReturn(response, response2);

        createProcessor();

        String next = runProcessor().get(0);
        assertThat(next).isEqualTo(HIT_SOURCE);
    }

    @Test
    public void shouldUseScrollIdInFollowupScrollRequest() throws Exception {
        SearchHit hit = new SearchHit(0, "id-0", new Text("ignored"), emptyMap());
        hit.sourceRef(new BytesArray(HIT_SOURCE));
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{hit}, new TotalHits(3, EQUAL_TO), Float.NaN));

        SearchResponse response2 = mock(SearchResponse.class);
        SearchHit hit2 = new SearchHit(1, "id-1", new Text("ignored"), emptyMap());
        hit2.sourceRef(new BytesArray(HIT_SOURCE2));
        when(response2.getHits()).thenReturn(new SearchHits(new SearchHit[]{hit2}, new TotalHits(3, EQUAL_TO), Float.NaN));

        SearchResponse response3 = mock(SearchResponse.class);
        when(response3.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, new TotalHits(3, EQUAL_TO), Float.NaN));
        when(mockClient.scroll(any(), any())).thenReturn(response2, response3);

        createProcessor();

        List<String> items = runProcessor();
        assertThat(items).containsExactly(HIT_SOURCE, HIT_SOURCE2);

        ArgumentCaptor<SearchScrollRequest> captor = forClass(SearchScrollRequest.class);

        verify(mockClient, times(2)).scroll(captor.capture(), any());
        SearchScrollRequest request = captor.getValue();
        assertThat(request.scrollId()).isEqualTo(SCROLL_ID);
        assertThat(request.scroll().keepAlive().getStringRep()).isEqualTo(KEEP_ALIVE);
    }

    @Test
    public void shouldUseOptionsFnForScroll() throws Exception {
        SearchHit hit = new SearchHit(0, "id-0", new Text("ignored"), emptyMap());
        hit.sourceRef(new BytesArray(HIT_SOURCE));
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{hit}, new TotalHits(1, EQUAL_TO), Float.NaN));

        SearchResponse response2 = mock(SearchResponse.class);
        when(response2.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, new TotalHits(1, EQUAL_TO), Float.NaN));
        when(mockClient.scroll(any(), any())).thenReturn(response2);

        // get different instance than default
        createProcessor(request -> {
            Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.addHeader("TestHeader", "value");
            return builder.build();
        });

        runProcessor();

        ArgumentCaptor<RequestOptions> captor = forClass(RequestOptions.class);
        verify(mockClient).scroll(any(), captor.capture());

        RequestOptions capturedOptions = captor.getValue();
        assertThat(capturedOptions.getHeaders())
                .extracting(h -> tuple(h.getName(), h.getValue()))
                .containsExactly(tuple("TestHeader", "value"));
    }

    @Test
    public void shouldUseLocalNodeOnly() throws Exception {
        RestClient lowClient = mock(RestClient.class);
        when(mockClient.getLowLevelClient()).thenReturn(lowClient);
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, new TotalHits(0, EQUAL_TO), Float.NaN));

        createProcessor(newArrayList(new Shard("my-index", 0, Prirep.p, 42,
                "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1")));

        runProcessor();

        ArgumentCaptor<Collection<Node>> nodesCaptor = ArgumentCaptor.forClass(Collection.class);

        verify(lowClient).setNodes(nodesCaptor.capture());

        Collection<Node> nodes = nodesCaptor.getValue();
        assertThat(nodes).hasSize(1);

        Node node = nodes.iterator().next();
        assertThat(node.getHost().toHostString()).isEqualTo("10.0.0.1:9200");
    }

    @Test
    public void shouldSearchShardsWithPreference() throws Exception {
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, new TotalHits(0, EQUAL_TO), Float.NaN));

        createProcessor(newArrayList(
                new Shard("my-index", 0, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1"),
                new Shard("my-index", 1, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1"),
                new Shard("my-index", 2, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1")
        ));

        runProcessor();

        ArgumentCaptor<SearchRequest> captor = forClass(SearchRequest.class);
        verify(mockClient).search(captor.capture(), any());

        SearchRequest request = captor.getValue();
        assertThat(request.preference()).isEqualTo("_shards:0,1,2|_only_local");
    }

    /*
     * Need to pass a Serializable Supplier into
     * ElasticsearchSourceBuilder.clientSupplier(...)
     * which returns a mock, so the mock itself must be serializable.
     *
     * Can't use Mockito's withSettings().serializable() because some of the setup (SearchResponse) is not Serializable
     */
    static class SerializableRestClient extends RestHighLevelClient implements Serializable {

        static SerializableRestClient instanceHolder;

        SerializableRestClient(RestClientBuilder restClientBuilder) {
            super(restClientBuilder);
        }

    }
}
