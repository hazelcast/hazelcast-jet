/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.function.FunctionEx;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SinkImpl<T> implements Sink<T> {

    private final String name;
    private final ProcessorMetaSupplier metaSupplier;
    private final boolean isTotalParallelismOne;
    private final FunctionEx<? super T, ?> inputPartitionKeyFunction;
    private boolean isAssignedToStage;

    public SinkImpl(
            @Nonnull String name,
            @Nonnull ProcessorMetaSupplier metaSupplier,
            boolean isTotalParallelismOne,
            @Nullable FunctionEx<? super T, ?> inputPartitionKeyFunction
    ) {
        if (inputPartitionKeyFunction != null && isTotalParallelismOne) {
            throw new IllegalArgumentException();
        }
        this.name = name;
        this.metaSupplier = metaSupplier;
        this.isTotalParallelismOne = isTotalParallelismOne;
        this.inputPartitionKeyFunction = inputPartitionKeyFunction;
    }

    @Nonnull
    public ProcessorMetaSupplier metaSupplier() {
        return metaSupplier;
    }

    public boolean isTotalParallelismOne() {
        return isTotalParallelismOne;
    }

    public FunctionEx<? super T, ?> inputPartitionKeyFunction() {
        return inputPartitionKeyFunction;
    }

    @Override
    public String name() {
        return name;
    }

    void onAssignToStage() {
        if (isAssignedToStage) {
            throw new IllegalStateException("Sink " + name + " was already assigned to a sink stage");
        }
        isAssignedToStage = true;
    }
}
