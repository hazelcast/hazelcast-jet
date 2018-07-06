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

import com.hazelcast.jet.impl.util.ExceptionUtil;

import java.io.Serializable;
import java.util.function.IntToLongFunction;

/**
 * {@code Serializable} variant of {@link IntToLongFunction
 * java.util.function.IntToLongFunction}  which throws checked exception.
 */
@FunctionalInterface
public interface DistributedIntToLongFunction extends IntToLongFunction, Serializable {

    /**
     * Exception-declaring version of {@link IntToLongFunction#applyAsLong}.
     */
    long applyAsLongEx(int value) throws Exception;

    @Override
    default long applyAsLong(int value) {
        try {
            return applyAsLongEx(value);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
