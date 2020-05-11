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
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.logging.ILogger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static java.util.Objects.requireNonNull;

/**
 * Builder for Elasticsearch Sink
 * <p>
 * The Sink first maps items from the pipeline using the provided
 * {@link #mapToRequestFn(FunctionEx)} and then using {@link BulkRequest}.
 * <p>
 * {@link BulkRequest#BulkRequest()} is used by default, it can be
 * modified by providing custom {@link #bulkRequestFn(SupplierEx)}
 *
 * <p>
 * Usage:
 * <pre>{@code
 * Sink<Map<String, ?>> elasticSink = new ElasticSinkBuilder<Map<String, ?>>()
 *   .clientFn(() -> ElasticClients.client(host, port))
 *   .mapToRequestFn(item -> new IndexRequest("my-index").source(item))
 *   .build();
 * }</pre>
 * <p>
 * Requires {@link #clientFn(SupplierEx)} and {@link #mapToRequestFn(FunctionEx)}.
 *
 * @param <T>
 * @since 4.2
 */
public class ElasticSinkBuilder<T> implements Serializable {

    private static final String DEFAULT_NAME = "elastic";

    private SupplierEx<? extends RestHighLevelClient> clientFn;
    private ConsumerEx<? super RestHighLevelClient> destroyFn = RestHighLevelClient::close;
    private SupplierEx<BulkRequest> bulkRequestFn = BulkRequest::new;
    private FunctionEx<? super T, ? extends DocWriteRequest<?>> mapToRequestFn;
    private FunctionEx<? super ActionRequest, RequestOptions> optionsFn = (request) -> RequestOptions.DEFAULT;
    private int preferredLocalParallelism = 2;

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
    public ElasticSinkBuilder<T> clientFn(@Nonnull SupplierEx<? extends RestHighLevelClient> clientFn) {
        this.clientFn = checkNonNullAndSerializable(clientFn, "clientFn");
        return this;
    }

    /**
     * Set the destroy function called on completion, defaults to {@link RestHighLevelClient#close()}
     *
     * @param destroyFn destroy function
     */
    @Nonnull
    public ElasticSinkBuilder<T> destroyFn(@Nonnull ConsumerEx<? super RestHighLevelClient> destroyFn) {
        this.destroyFn = checkNonNullAndSerializable(destroyFn, "destroyFn");
        return this;
    }

    /**
     * Set the supplier function for BulkRequest, defaults to new {@link BulkRequest#BulkRequest()}
     * <p>
     * For example, to modify the BulkRequest used to index documents:
     * <pre>{@code
     * builder.bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(IMMEDIATE))
     * }</pre>
     *
     * @param bulkRequestFn supplier function for the bulk request
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#bulk">
     * Bulk indexing usage</a> in Elastic documentation
     */
    @Nonnull
    public ElasticSinkBuilder<T> bulkRequestFn(@Nonnull SupplierEx<BulkRequest> bulkRequestFn) {
        this.bulkRequestFn = checkNonNullAndSerializable(bulkRequestFn, "bulkRequestFn");
        return this;
    }

    /**
     * Set the function mapping the item from a pipeline item to an index request
     * <p>
     * For example, to create an IndexRequest for a versioned document:
     * <pre>{@code
     * builder.mapToRequestFn((mapItem) ->
     *                      new IndexRequest("my-index")
     *                              .source(map)
     *                              .version((Long) map.get("version"))
     * }</pre>
     *
     * @param mapToRequestFn maps an item from the stream to an {@link org.elasticsearch.action.index.IndexRequest},
     *                       {@link org.elasticsearch.action.update.UpdateRequest} or
     *                       {@link org.elasticsearch.action.delete.DeleteRequest}
     */
    @Nonnull
    public ElasticSinkBuilder<T> mapToRequestFn(@Nonnull FunctionEx<? super T, ? extends DocWriteRequest<?>> mapToRequestFn) {
        this.mapToRequestFn = checkNonNullAndSerializable(mapToRequestFn, "mapToRequestFn");
        return this;
    }

    /**
     * Set the function that provides {@link RequestOptions}
     * <p>
     * It can either return a constant value or a value based on provided request.
     * <p>
     * For example, to provide a custom authentication header:
     * <pre>{@code
     * sinkBuilder.optionsFn((request) -> {
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
     */
    @Nonnull
    public ElasticSinkBuilder<T> optionsFn(@Nonnull FunctionEx<? super ActionRequest, RequestOptions> optionsFn) {
        this.optionsFn = checkNonNullAndSerializable(optionsFn, "optionsFn");
        return this;
    }

    /**
     * Set the local parallelism of this sink. See {@link SinkBuilder#preferredLocalParallelism(int)}.
     */
    @Nonnull
    public ElasticSinkBuilder<T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = preferredLocalParallelism;
        return this;
    }

    /**
     * Create a sink that writes data into Elasticsearch based on this builder configuration
     */
    @Nonnull
    public Sink<T> build() {
        requireNonNull(clientFn, "clientFn is not set");
        requireNonNull(mapToRequestFn, "mapToRequestFn is not set");

        return SinkBuilder
                .sinkBuilder(DEFAULT_NAME, ctx ->
                        new BulkContext(clientFn.get(), bulkRequestFn,
                                optionsFn, destroyFn, ctx.logger()))
                .<T>receiveFn((bulkContext, item) -> bulkContext.add(mapToRequestFn.apply(item)))
                .flushFn(BulkContext::flush)
                .destroyFn(BulkContext::close)
                .preferredLocalParallelism(preferredLocalParallelism)
                .build();
    }

    static final class BulkContext {

        private final RestHighLevelClient client;
        private final SupplierEx<BulkRequest> bulkRequestSupplier;
        private final FunctionEx<? super ActionRequest, RequestOptions> optionsFn;
        private final ConsumerEx<? super RestHighLevelClient> destroyFn;

        private BulkRequest bulkRequest;
        private final ILogger logger;

        BulkContext(RestHighLevelClient client, SupplierEx<BulkRequest> bulkRequestSupplier,
                    FunctionEx<? super ActionRequest, RequestOptions> optionsFn,
                    ConsumerEx<? super RestHighLevelClient> destroyFn, ILogger logger) {
            this.client = client;
            this.bulkRequestSupplier = bulkRequestSupplier;
            this.optionsFn = optionsFn;
            this.destroyFn = destroyFn;

            this.bulkRequest = bulkRequestSupplier.get();
            this.logger = logger;
        }

        void add(DocWriteRequest<?> request) {
            bulkRequest.add(request);
        }

        void flush() throws IOException {
            if (!bulkRequest.requests().isEmpty()) {
                BulkResponse response = client.bulk(bulkRequest, optionsFn.apply(bulkRequest));
                if (response.hasFailures()) {
                    throw new ElasticsearchException(response.buildFailureMessage());
                }
                if (logger.isFineEnabled()) {
                    logger.fine("BulkRequest with " + bulkRequest.requests().size() + " requests succeeded");
                }
                bulkRequest = bulkRequestSupplier.get();
            }
        }

        void close() throws IOException {
            logger.fine("Closing BulkContext");
            flush();
            destroyFn.accept(client);
        }
    }

}
