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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nonnull;
import java.io.Serializable;

public class ElasticSourceConfiguration<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SupplierEx<? extends RestHighLevelClient> clientSupplier;
    private final ConsumerEx<? super RestHighLevelClient> destroyFn;
    private final SupplierEx<SearchRequest> searchRequestSupplier;
    private final FunctionEx<? super ActionRequest, RequestOptions> optionsFn;
    private final FunctionEx<? super SearchHit, T> mapHitFn;
    private final boolean slicing;
    private final boolean coLocatedReading;
    private final String scrollKeepAlive;
    private final int preferredLocalParallelism;

    public ElasticSourceConfiguration(SupplierEx<? extends RestHighLevelClient> clientSupplier, ConsumerEx<?
            super RestHighLevelClient> destroyFn, SupplierEx<SearchRequest> searchRequestSupplier, FunctionEx<?
            super ActionRequest, RequestOptions> optionsFn, FunctionEx<? super SearchHit, T> mapHitFn,
                                      boolean slicing, boolean coLocatedReading, String scrollKeepAlive,
                                      int preferredLocalParallelism) {
        this.clientSupplier = clientSupplier;
        this.destroyFn = destroyFn;
        this.searchRequestSupplier = searchRequestSupplier;
        this.optionsFn = optionsFn;
        this.mapHitFn = mapHitFn;
        this.slicing = slicing;
        this.coLocatedReading = coLocatedReading;
        this.scrollKeepAlive = scrollKeepAlive;
        this.preferredLocalParallelism = preferredLocalParallelism;
    }

    @Nonnull
    public SupplierEx<? extends RestHighLevelClient> clientSupplier() {
        return clientSupplier;
    }


    @Nonnull
    public ConsumerEx<? super RestHighLevelClient> destroyFn() {
        return destroyFn;
    }


    @Nonnull
    public SupplierEx<SearchRequest> searchRequestSupplier() {
        return searchRequestSupplier;
    }


    @Nonnull
    public FunctionEx<? super SearchHit, T> mapHitFn() {
        return mapHitFn;
    }


    public FunctionEx<? super ActionRequest, RequestOptions> optionsFn() {
        return optionsFn;
    }

    public boolean slicing() {
        return slicing;
    }

    public boolean coLocatedReading() {
        return coLocatedReading;
    }

    public String scrollKeepAlive() {
        return scrollKeepAlive;
    }

    public int preferredLocalParallelism() {
        return preferredLocalParallelism;
    }

}
