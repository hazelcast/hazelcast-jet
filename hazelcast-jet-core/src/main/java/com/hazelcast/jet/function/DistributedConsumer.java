/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.function;

import java.io.Serializable;
import java.util.function.Consumer;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link Consumer
 * java.util.function.Consumer}.
 */
@FunctionalInterface
public interface DistributedConsumer<T> extends Consumer<T>, Serializable {

    /**
     * {@code Serializable} variant of {@link Consumer#andThen(Consumer)
     * java.util.function.Consumer#andThen(Consumer)}.
     */
    default DistributedConsumer<T> andThen(DistributedConsumer<? super T> after) {
        checkNotNull(after, "after");
        return t -> {
            accept(t);
            after.accept(t);
        };
    }
}
