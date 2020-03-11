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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.contrib.elasticsearch.impl.ElasticProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Builder for Elasticsearch source which reads data from Elasticsearch and
 * converts SearchHits using provided {@code mapHitFn}
 *
 * @param <T> type of the mapping function from {@link SearchHit} -> T
 *           TODO not sure about the type parameter name - T as the usual default, or R for Result
 *           also we could accept the function in the build() method, same as the original source did it
 * @since 4.1
 */
public class ElasticsearchSourceBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name = "elastic";
    private SupplierEx<? extends RestHighLevelClient> clientSupplier;
    private ConsumerEx<? super RestHighLevelClient> destroyFn = RestHighLevelClient::close;
    private SupplierEx<SearchRequest> searchRequestSupplier;
    private FunctionEx<? super ActionRequest, RequestOptions> optionsFn = request -> RequestOptions.DEFAULT;
    private FunctionEx<SearchHit, T> mapHitFn;
    private boolean slicing;
    private boolean coLocatedReading;
    private String scrollKeepAlive = "1m"; // Using String because it needs to be Serializable

    /**
     * Build Elasticsearch {@link BatchSource} with supplied parameters
     *
     * @return configured source which is to be used in the pipeline
     */
    @Nonnull
    public BatchSource<T> build() {
        ElasticProcessorMetaSupplier<T> metaSupplier = new ElasticProcessorMetaSupplier<>(this);
        return Sources.batchFromProcessor(name, metaSupplier);
    }

    /**
     * Set the user-friendly source name for this source
     *
     * @param sourceName user-friendly source name
     */
    @Nonnull
    public ElasticsearchSourceBuilder<T> name(String sourceName) {
        this.name = sourceName;
        return this;
    }

    @Nonnull
    public String name() {
        return name;
    }

    /**
     * Set the client supplier
     *
     * @param clientSupplier supplier for configure Elasticsearch REST client
     */
    @Nonnull
    public ElasticsearchSourceBuilder<T> clientSupplier(SupplierEx<? extends RestHighLevelClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        return this;
    }

    @Nonnull
    public SupplierEx<? extends RestHighLevelClient> clientSupplier() {
        return clientSupplier;
    }

    @Nonnull
    public ElasticsearchSourceBuilder<T> destroyFn(ConsumerEx<? super RestHighLevelClient> destroyFn) {
        this.destroyFn = destroyFn;
        return this;
    }

    @Nonnull
    public ConsumerEx<? super RestHighLevelClient> destroyFn() {
        return destroyFn;
    }

    /**
     * Set the search request supplier
     *
     * @param searchRequestSupplier search request supplier
     */
    @Nonnull
    public ElasticsearchSourceBuilder<T> searchRequestSupplier(SupplierEx<SearchRequest> searchRequestSupplier) {
        this.searchRequestSupplier = searchRequestSupplier;
        return this;
    }

    @Nonnull
    public SupplierEx<SearchRequest> searchRequestSupplier() {
        return searchRequestSupplier;
    }

    /**
     * Set the function to map SearchHit to custom result
     *
     * @param mapHitFn maps search hits to output items
     */
    @Nonnull
    public ElasticsearchSourceBuilder<T> mapHitFn(FunctionEx<SearchHit, T> mapHitFn) {
        this.mapHitFn = mapHitFn;
        return this;
    }

    @Nonnull
    public FunctionEx<SearchHit, T> mapHitFn() {
        return mapHitFn;
    }

    @Nonnull
    public ElasticsearchSourceBuilder<T> optionsFn(FunctionEx<? super ActionRequest, RequestOptions> optionsFn) {
        this.optionsFn = optionsFn;
        return this;
    }

    public FunctionEx<? super ActionRequest, RequestOptions> optionsFn() {
        return optionsFn;
    }

    /**
     * Set to true to enable slicing
     * <p>
     * Number of slices is equal to globalParallelism (localParallelism * numberOfNodes)
     * <p>
     * Use this option to read from multiple shards in parallel.
     *
     * @param enabled {@code true} to enable slicing, default value {@code false}
     * @see
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#sliced-scroll">
     *     Sliced Scroll</a>
     */
    @Nonnull
    public ElasticsearchSourceBuilder<T> slicing(boolean enabled) {
        this.slicing = enabled;
        return this;
    }

    public boolean slicing() {
        return slicing;
    }

    /**
     * Turns on co-located reading
     *
     * Jet cluster member must run exactly on the same nodes as Elastic cluster.
     *
     * @param coLocatedRead {@code true} to enable co-located reading, default value {@code false}
     */
    @Nonnull
    public ElasticsearchSourceBuilder<T> coLocatedReading(boolean coLocatedRead) {
        this.coLocatedReading = coLocatedRead;
        return this;
    }

    public boolean coLocatedReading() {
        return coLocatedReading;
    }

    /**
     * Set the keepAlive for Elastic search scroll
     * <p>
     * See {@link SearchRequest#scroll(String)}
     *
     * @param scrollKeepAlive keepAlive value, this must be high enough to process all resuls from a single scroll,
     *                        default value 1m
     */
    @Nonnull
    public ElasticsearchSourceBuilder<T> scrollKeepAlive(String scrollKeepAlive) {
        this.scrollKeepAlive = scrollKeepAlive;
        return this;
    }

    public String scrollKeepAlive() {
        return scrollKeepAlive;
    }

}
