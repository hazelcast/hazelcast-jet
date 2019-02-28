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

package com.hazelcast.jet.core.processor;

import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.JetEvent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

import static com.hazelcast.jet.impl.JetEvent.jetEvent;

@FunctionalInterface
public interface KeyedWindowResultFunction<K, R, OUT> extends Serializable {

    @Nullable
    OUT apply(long winStart, long winEnd, @Nonnull K key, @Nonnull R windowResult, boolean isEarly);

    static <R> JetEvent<WindowResult<R>> windowResult(
            long winStart, long winEnd, @Nullable Object key, @Nonnull R windowResult, boolean isEarly
    ) {
        return jetEvent(winEnd - 1, new WindowResult<>(winStart, winEnd, windowResult, isEarly));
    }

    static <K, R> JetEvent<KeyedWindowResult<K, R>> keyedWindowResult(
            long winStart, long winEnd, @Nonnull K key, @Nonnull R windowResult, boolean isEarly
    ) {
        return jetEvent(winEnd - 1, new KeyedWindowResult<>(winStart, winEnd, key, windowResult, isEarly));
    }
}
