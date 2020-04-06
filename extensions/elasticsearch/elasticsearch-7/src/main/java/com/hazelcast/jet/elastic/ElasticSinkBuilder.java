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

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Objects.requireNonNull;

/**
 * Builder for Elasticsearch Sink
 *
 * Requires {@link #clientSupplier(SupplierEx)} and {@link #mapItemFn(FunctionEx)}.
 *
 * @param <T>
 *
 * @since 4.1
 */
public class ElasticSinkBuilder<T> implements Serializable {

    private String name = "elastic";
    private SupplierEx<? extends RestHighLevelClient> clientSupplier;
    private ConsumerEx<? super RestHighLevelClient> destroyFn = RestHighLevelClient::close;
    private SupplierEx<BulkRequest> bulkRequestSupplier = BulkRequest::new;
    private FunctionEx<? super T, ? extends DocWriteRequest<?>> mapItemFn;
    private FunctionEx<? super ActionRequest, RequestOptions> optionsFn = (request) -> RequestOptions.DEFAULT;
    private int preferredLocalParallelism = 2;

    /**
     * Set the user-friendly source name for this sink
     *
     * @param sinkName user-friendly sink name
     */
    @Nonnull
    public ElasticSinkBuilder<T> name(@Nonnull String sinkName) {
        this.name = requireNonNull(sinkName, "sinkName");
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
    public ElasticSinkBuilder<T> clientSupplier(SupplierEx<? extends RestHighLevelClient> clientSupplier) {
        checkSerializable(clientSupplier, "clientSupplier");
        this.clientSupplier = clientSupplier;
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
    public ElasticSinkBuilder<T> destroyFn(ConsumerEx<? super RestHighLevelClient> destroyFn) {
        checkSerializable(destroyFn, "destroyFn");
        this.destroyFn = destroyFn;
        return this;
    }

    @Nonnull
    public ConsumerEx<? super RestHighLevelClient> destroyFn() {
        return destroyFn;
    }

    /**
     * Set the bulkRequestSupplier, defaults to new {@link BulkRequest#BulkRequest()}
     *
     * @param bulkRequestSupplier supplier for the bulk request
     */
    @Nonnull
    public ElasticSinkBuilder<T> bulkRequestSupplier(SupplierEx<BulkRequest> bulkRequestSupplier) {
        checkSerializable(bulkRequestSupplier, "clientSupplier");
        this.bulkRequestSupplier = bulkRequestSupplier;
        return this;
    }

    @Nonnull
    public SupplierEx<BulkRequest> bulkRequestSupplier() {
        return bulkRequestSupplier;
    }

    /**
     * Set the mapItemFn
     *
     * @param mapItemFn maps an item from the stream to an {@link org.elasticsearch.action.index.IndexRequest},
     *                  {@link org.elasticsearch.action.update.UpdateRequest} or
     *                  {@link org.elasticsearch.action.delete.DeleteRequest}
     */
    @Nonnull
    public ElasticSinkBuilder<T> mapItemFn(FunctionEx<? super T, ? extends DocWriteRequest<?>> mapItemFn) {
        checkSerializable(mapItemFn, "mapItemFn");
        this.mapItemFn = mapItemFn;
        return this;
    }

    public FunctionEx<? super T, ? extends DocWriteRequest<?>> mapItemFn() {
        return mapItemFn;
    }

    /**
     * Set the function that provides {@link RequestOptions} based on given request
     *
     * @param optionsFn function that provides {@link RequestOptions}
     */
    @Nonnull
    public ElasticSinkBuilder<T> optionsFn(FunctionEx<? super ActionRequest, RequestOptions> optionsFn) {
        checkSerializable(optionsFn, "optionsFn");
        this.optionsFn = optionsFn;
        return this;
    }

    public FunctionEx<? super ActionRequest, RequestOptions> optionsFn() {
        return optionsFn;
    }

    @Nonnull
    public ElasticSinkBuilder<T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = preferredLocalParallelism;
        return this;
    }

    public int preferredLocalParallelism() {
        return preferredLocalParallelism;
    }

    /**
     * Create a sink that writes data into Elasticsearch based on this builder configuration
     */
    public Sink<T> build() {
        requireNonNull(clientSupplier, "clientSupplier is not set");
        requireNonNull(mapItemFn, "mapItemFn is not set");

        return SinkBuilder
                .sinkBuilder(name, ctx ->
                        new BulkContext(clientSupplier.get(), bulkRequestSupplier,
                                optionsFn, destroyFn, ctx.logger()))
                .<T>receiveFn((bulkContext, item) -> bulkContext.add(mapItemFn.apply(item)))
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
//            System.out.println("add" + request.id());
            bulkRequest.add(request);
        }

        void flush() throws IOException {
            if (!bulkRequest.requests().isEmpty()) {
                BulkResponse response = client.bulk(bulkRequest, optionsFn.apply(bulkRequest));
                if (response.hasFailures()) {
                    System.out.println("BulkRequest with " + bulkRequest.requests().size() + " requests failed");
                    throw new ElasticsearchException(response.buildFailureMessage());
                }
                System.out.println("BulkRequest with " + bulkRequest.requests().size() + " requests succeeded");
//                logger.fine("BulkRequest with " + bulkRequest.requests().size() + " requests succeeded");
                bulkRequest = bulkRequestSupplier.get();
            }
        }

        void close() throws IOException {
            flush();
            destroyFn.accept(client);
        }
    }

}
