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
import com.hazelcast.jet.elastic.impl.ElasticSourcePMetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sources;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Objects.requireNonNull;

/**
 * Builder for Elasticsearch source which reads data from Elasticsearch and
 * converts SearchHits using provided {@code mapToItemFn}
 * <p>
 * Usage:
 * <pre>{@code
 * BatchSource<String> source = new ElasticSourceBuilder<String>()
 *   .clientFn(() -> client(host, port))
 *   .searchRequestFn(() -> new SearchRequest("my-index"))
 *   .mapToItemFn(SearchHit::getSourceAsString)
 *   .build();
 *
 * BatchStage<String> stage = p.readFrom(source);
 * }</pre>
 *
 * @param <T> type of the output of the mapping function from {@link SearchHit} -> T
 * @since 4.2
 */
public class ElasticSourceBuilder<T> {

    private static final String DEFAULT_NAME = "elastic";

    private SupplierEx<? extends RestHighLevelClient> clientFn;
    private ConsumerEx<? super RestHighLevelClient> destroyFn = RestHighLevelClient::close;
    private SupplierEx<SearchRequest> searchRequestFn;
    private FunctionEx<? super ActionRequest, RequestOptions> optionsFn = request -> RequestOptions.DEFAULT;
    private FunctionEx<? super SearchHit, T> mapToItemFn;
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
        requireNonNull(clientFn, "clientFn must be set");
        requireNonNull(searchRequestFn, "searchRequestFn must be set");
        requireNonNull(mapToItemFn, "mapToItemFn must be set");

        ElasticSourceConfiguration<T> configuration = new ElasticSourceConfiguration<T>(
                clientFn, destroyFn, searchRequestFn, optionsFn, mapToItemFn, slicing, coLocatedReading,
                scrollKeepAlive, preferredLocalParallelism
        );
        ElasticSourcePMetaSupplier<T> metaSupplier = new ElasticSourcePMetaSupplier<T>(configuration);
        return Sources.batchFromProcessor(DEFAULT_NAME, metaSupplier);
    }

    /**
     * Set the client supplier function
     * <p>
     * The connector uses the returned instance to access Elasticsearch. Also see {@link ElasticClients} for convenience
     * factory methods.
     * <p>
     * For example, to provide an authenticated client:
     * <pre>{@code
     * builder.clientFn(() -> client(host, port, username, password))
     * }</pre>
     *
     * @param clientFn supplier function returning configured Elasticsearch REST client
     */
    @Nonnull
    public ElasticSourceBuilder<T> clientFn(@Nonnull SupplierEx<? extends RestHighLevelClient> clientFn) {
        this.clientFn = checkNonNullAndSerializable(clientFn, "clientFn");
        return this;
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

    /**
     * Set the search request supplier function
     * <p>
     * The connector executes this search request to retrieve documents from Elasticsearch.
     * <p>
     * For example, to create SearchRequest limited to an index `logs`:
     * <pre>{@code
     * builder.searchRequestFn(() -> new SearchRequest("logs"))
     * }</pre>
     *
     * @param searchRequestFn search request supplier function
     */
    @Nonnull
    public ElasticSourceBuilder<T> searchRequestFn(@Nonnull SupplierEx<SearchRequest> searchRequestFn) {
        this.searchRequestFn = checkSerializable(searchRequestFn, "searchRequestFn");
        return this;
    }

    /**
     * Set the function to map SearchHit to a pipeline item
     * <p>
     * For example, to map a SearchHit to a value of a field `productId`:
     * <pre>{@code
     * builder.mapToItemFn(hit -> (String) hit.getSourceAsMap().get("productId"))
     * }</pre>
     *
     * @param mapToItemFn maps search hits to output items
     */
    @Nonnull
    public ElasticSourceBuilder<T> mapToItemFn(@Nonnull FunctionEx<? super SearchHit, T> mapToItemFn) {
        this.mapToItemFn = checkSerializable(mapToItemFn, "mapToItemFn");
        return this;
    }

    /**
     * Set the function that provides {@link RequestOptions}
     * <p>
     * It can either return a constant value or a value based on provided request.
     * <p>
     * For example, use this to provide a custom authentication header:
     * <pre>{@code
     * sourceBuilder.optionsFn((request) -> {
     *     RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
     *     builder.addHeader("Authorization", "Bearer " + TOKEN);
     *     return builder.build();
     * })
     * }</pre>
     *
     * @param optionsFn function that provides {@link RequestOptions}
     * @see
     * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html#java-rest-low-usage-request-options">
     * RequestOptions</a> in Elastic documentation
     */    @Nonnull
    public ElasticSourceBuilder<T> optionsFn(@Nonnull FunctionEx<? super ActionRequest, RequestOptions> optionsFn) {
        this.optionsFn = checkSerializable(optionsFn, "optionsFn");
        return this;
    }

    /**
     * Enable slicing
     * <p>
     * Number of slices is equal to globalParallelism (localParallelism * numberOfNodes)
     * <p>
     * Use this option to read from multiple shards in parallel.
     *
     * @see
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#sliced-scroll">
     *     Sliced Scroll</a>
     */
    @Nonnull
    public ElasticSourceBuilder<T> enableSlicing() {
        this.slicing = true;
        return this;
    }

    /**
     * Enable co-located reading
     *
     * Jet cluster member must run exactly on the same nodes as Elastic cluster.
     */
    @Nonnull
    public ElasticSourceBuilder<T> enableCoLocatedReading() {
        this.coLocatedReading = true;
        return this;
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

    /**
     * Set the local parallelism of this source.
     *
     * Note this has an effect only when {@link #enableCoLocatedReading()} or {@link #enableSlicing()} is enabled,
     * otherwise the data is read only from 1 processor in whole Jet cluster.
     */
    @Nonnull
    public ElasticSourceBuilder<T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = preferredLocalParallelism;
        return this;
    }

}
