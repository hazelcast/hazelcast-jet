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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.elastic.impl.ElasticProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Objects.requireNonNull;

/**
 * Builder for Elasticsearch source which reads data from Elasticsearch and
 * converts SearchHits using provided {@code mapHitFn}
 *
 * Usage:
 * <pre>{@code
 * BatchSource<String> source = new ElasticSourceBuilder<String>()
 *   .clientSupplier(() -> client(host, port))
 *   .searchRequestSupplier(() -> new SearchRequest("my-index"))
 *   .mapHitFn(SearchHit::getSourceAsString)
 *   .build();
 *
 * BatchStage<String> stage = p.readFrom(source);
 * }</pre>
 * @param <T> type of the mapping function from {@link SearchHit} -> T
 * @since 4.1
 */
public class ElasticSourceBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_NAME = "elastic";

    private SupplierEx<? extends RestHighLevelClient> clientSupplier;
    private ConsumerEx<? super RestHighLevelClient> destroyFn = RestHighLevelClient::close;
    private SupplierEx<SearchRequest> searchRequestSupplier;
    private FunctionEx<? super ActionRequest, RequestOptions> optionsFn = request -> RequestOptions.DEFAULT;
    private FunctionEx<? super SearchHit, T> mapHitFn;
    private boolean slicing;
    private boolean coLocatedReading;
    private String scrollKeepAlive = "1m"; // Using String because it needs to be Serializable
    private int preferredLocalParallelism = 2;

    /**
     * Build Elasticsearch {@link BatchSource} with supplied parameters
     *
     * @return configured source which is to be used in the pipeline
     */
    @Nonnull
    public BatchSource<T> build() {
        requireNonNull(clientSupplier, "clientSupplier must be set");
        requireNonNull(searchRequestSupplier, "searchRequestSupplier must be set");
        requireNonNull(mapHitFn, "mapHitFn must be set");

        ElasticProcessorMetaSupplier<T> metaSupplier = new ElasticProcessorMetaSupplier<>(this);
        return Sources.batchFromProcessor(DEFAULT_NAME, metaSupplier);
    }

    /**
     * Set the client supplier
     *
     * @param clientSupplier supplier for configure Elasticsearch REST client
     */
    @Nonnull
    public ElasticSourceBuilder<T> clientSupplier(@Nonnull SupplierEx<? extends RestHighLevelClient> clientSupplier) {
        this.clientSupplier = checkNonNullAndSerializable(clientSupplier, "clientSupplier");
        return this;
    }

    @Nonnull
    public SupplierEx<? extends RestHighLevelClient> clientSupplier() {
        return clientSupplier;
    }

    /**
     * Set the destroy function called on completion, defaults to {@link RestHighLevelClient#close()}
     *
     * @param destroyFn destroy function
     */
    @Nonnull
    public ElasticSourceBuilder<T> destroyFn(@Nonnull ConsumerEx<? super RestHighLevelClient> destroyFn) {
        this.destroyFn = checkNonNullAndSerializable(destroyFn, "destroyFn");
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
    public ElasticSourceBuilder<T> searchRequestSupplier(@Nonnull SupplierEx<SearchRequest> searchRequestSupplier) {
        this.searchRequestSupplier = checkSerializable(searchRequestSupplier, "searchRequestSupplier");
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
    public ElasticSourceBuilder<T> mapHitFn(@Nonnull FunctionEx<? super SearchHit, T> mapHitFn) {
        this.mapHitFn = checkSerializable(mapHitFn, "mapHitFn");
        return this;
    }

    @Nonnull
    public FunctionEx<? super SearchHit, T> mapHitFn() {
        return mapHitFn;
    }

    /**
     * Set the function that provides {@link RequestOptions} based on given request
     *
     * @param optionsFn function that provides {@link RequestOptions}
     */
    @Nonnull
    public ElasticSourceBuilder<T> optionsFn(@Nonnull FunctionEx<? super ActionRequest, RequestOptions> optionsFn) {
        this.optionsFn = checkSerializable(optionsFn, "optionsFn");
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
    public ElasticSourceBuilder<T> slicing(boolean enabled) {
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
    public ElasticSourceBuilder<T> coLocatedReading(boolean coLocatedRead) {
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
     * @param scrollKeepAlive keepAlive value, this must be high enough to process all results from a single scroll,
     *                        default value 1m
     */
    @Nonnull
    public ElasticSourceBuilder<T> scrollKeepAlive(@Nonnull String scrollKeepAlive) {
        this.scrollKeepAlive = requireNonNull(scrollKeepAlive, scrollKeepAlive);
        return this;
    }

    public String scrollKeepAlive() {
        return scrollKeepAlive;
    }

    /**
     * Set the preferred local parallelism
     *
     * @param preferredLocalParallelism preferred local parallelism
     */
    @Nonnull
    public ElasticSourceBuilder<T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = preferredLocalParallelism;
        return this;
    }

    public int preferredLocalParallelism() {
        return preferredLocalParallelism;
    }
}
