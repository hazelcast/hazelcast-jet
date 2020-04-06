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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.elasticsearch.ElasticsearchSourceBuilder;
import com.hazelcast.logging.ILogger;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.slice.SliceBuilder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

final class ElasticProcessor<T> extends AbstractProcessor {

    private ElasticsearchSourceBuilder<T> builder;
    private final List<Shard> shards;
    private Traverser<T> traverser;

    ElasticProcessor(ElasticsearchSourceBuilder<T> builder) {
        this.builder = builder;
        this.shards = null;
        assert !builder.coLocatedReading() : "Co-located reading is on but no shards given";
    }

    ElasticProcessor(ElasticsearchSourceBuilder<T> builder, List<Shard> shards) {
        this.builder = builder;
        this.shards = shards;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);

        ILogger logger = context.logger();
        logger.fine("init");

        RestHighLevelClient client = builder.clientSupplier().get();
        SearchRequest sr = builder.searchRequestSupplier().get();
        sr.scroll(builder.scrollKeepAlive());

        int sliceId = context.globalProcessorIndex();
        int totalSlices = context.totalParallelism();

        if (builder.slicing() && totalSlices > 1) {
            logger.fine("Slice id=" + sliceId + ", max=" + totalSlices);
            sr.source().slice(new SliceBuilder(sliceId, totalSlices));
        }

        if (builder.coLocatedReading()) {
            logger.fine("Assigned shards: " + shards);
            if (shards.isEmpty()) {
                traverser = Traversers.empty();
                return;
            }

            Node node = createLocalElasticNode();
            client.getLowLevelClient().setNodes(singleton(node));
            String preference =
                    "_shards:" + shards.stream().map(shard -> String.valueOf(shard.getShard())).collect(joining(","))
                            + "|_only_local";
            sr.preference(preference);
        }

        traverser = new ElasticScrollTraverser<>(builder, client, sr, logger);
    }

    private Node createLocalElasticNode() {
        List<String> ips = shards.stream().map(Shard::getHttpAddress).distinct().collect(toList());
        if (ips.size() != 1) {
            throw new JetException("Should receive shards from single local node, got: " + ips);
        }
        String localIp = ips.get(0);
        return new Node(HttpHost.create(localIp));
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    @Override
    public void close() throws Exception {
        if (traverser instanceof ElasticProcessor.ElasticScrollTraverser) {
            ElasticScrollTraverser<T> scrollTraverser = (ElasticScrollTraverser<T>) traverser;
            scrollTraverser.close();
        }
    }

    static class ElasticScrollTraverser<R> implements Traverser<R> {

        private final ILogger logger;

        private final RestHighLevelClient client;
        private final FunctionEx<? super ActionRequest, RequestOptions> optionsFn;
        private final FunctionEx<? super SearchHit, R> mapHitFn;
        private final String scrollKeepAlive;
        private final ConsumerEx<? super RestHighLevelClient> destroyFn;

        private SearchHits hits;
        private int nextHit;
        private String scrollId;

        ElasticScrollTraverser(ElasticsearchSourceBuilder<R> builder, RestHighLevelClient client, SearchRequest sr,
                               ILogger logger) {
            this.client = client;
            this.optionsFn = builder.optionsFn();
            this.mapHitFn = builder.mapHitFn();
            this.destroyFn = builder.destroyFn();
            this.scrollKeepAlive = builder.scrollKeepAlive();
            this.logger = logger;

            try {
                RequestOptions options = optionsFn.apply(sr);
                SearchResponse response = this.client.search(sr, options);

                // These should be always present, even when there are no results
                hits = requireNonNull(response.getHits(), "null hits in the response");
                scrollId = requireNonNull(response.getScrollId(), "null scrollId in the response");

                TotalHits totalHits = hits.getTotalHits();
                logger.fine("Initialized scroll with scrollId " + scrollId + ", total results " +
                        totalHits.relation + ", " + totalHits.value);
            } catch (IOException e) {
                throw new JetException("Could not execute SearchRequest to Elastic", e);
            }
        }

        @Override
        public R next() {
            if (hits.getHits().length == 0) {
                scrollId = null;
                return null;
            }

            if (nextHit >= hits.getHits().length) {
                try {
                    SearchScrollRequest ssr = new SearchScrollRequest(scrollId);
                    ssr.scroll(scrollKeepAlive);

                    SearchResponse searchResponse = client.scroll(ssr, optionsFn.apply(ssr));
                    hits = searchResponse.getHits();
                    if (hits.getHits().length == 0) {
                        return null;
                    }
                    nextHit = 0;
                } catch (IOException e) {
                    throw new JetException("Could not execute SearchScrollRequest to Elastic", e);
                }
            }

            return mapHitFn.apply(hits.getAt(nextHit++));
        }

        public void close() {
            if (scrollId != null) {
                clearScroll(scrollId);
                scrollId = null;
            }

            try {
                destroyFn.accept(client);
            } catch (Exception e) { // IOException on client.close()
                logger.fine("Could not close client", e);
            }
        }

        private void clearScroll(String scrollId) {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            try {
                ClearScrollResponse response = client.clearScroll(clearScrollRequest,
                        optionsFn.apply(clearScrollRequest));

                if (response.isSucceeded()) {
                    logger.fine("Succeeded clearing " + response.getNumFreed() + " scrolls");
                } else {
                    logger.warning("Clearing scroll " + scrollId + " failed");
                }
            } catch (IOException e) {
                logger.fine("Could not clear scroll with scrollId=" + scrollId, e);
            }
        }
    }

}
