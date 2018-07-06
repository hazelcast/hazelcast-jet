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
import java.util.function.LongToIntFunction;

/**
 * {@code Serializable} variant of {@link LongToIntFunction
 * java.util.function.LongToIntFunction} which throws checked exception.
 */
@FunctionalInterface
public interface DistributedLongToIntFunction extends LongToIntFunction, Serializable {

    /**
     * Exception-declaring version of {@link LongToIntFunction#applyAsInt}.
     */
    int applyAsIntEx(long value) throws Exception;

    @Override
    default int applyAsInt(long value) {
        try {
            return applyAsIntEx(value);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
