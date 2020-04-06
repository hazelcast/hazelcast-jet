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

package com.hazelcast.jet.cdc.impl.util;

import java.io.Serializable;

/**
 * Serializable function (with one argument), one that can throw an
 * {@code Exception} of an explicitly specified type from its
 * {@code apply()} method.
 * <p>
 * <b>NOT</b> thread safe.
 *
 * @param <T> type of function parameter
 * @param <R> type of function result
 * @param <E> type of thrown exception
 *
 * @since 4.1
 */
@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Exception> extends Serializable {

    /**
     * Computes a result based on the input, potentially throwing an
     * {@code E extends Exception} during the process.
     */
    R apply(T t) throws E;

}