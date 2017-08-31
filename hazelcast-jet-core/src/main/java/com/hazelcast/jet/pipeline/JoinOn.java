/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.function.DistributedFunction;

import java.io.Serializable;

/**
 * Javadoc pending.
 */
public final class JoinOn<K, E_LEFT, E_RIGHT> implements Serializable {
    private final DistributedFunction<E_LEFT, K> leftKeyFn;
    private final DistributedFunction<E_RIGHT, K> rightKeyFn;

    private JoinOn(
            DistributedFunction<E_LEFT, K> leftKeyFn,
            DistributedFunction<E_RIGHT, K> rightKeyFn
    ) {
        this.leftKeyFn = leftKeyFn;
        this.rightKeyFn = rightKeyFn;
    }

    public static <K, E_LEFT, E_RIGHT> JoinOn<K, E_LEFT, E_RIGHT> onKeys(
            DistributedFunction<E_LEFT, K> leftKeyFn,
            DistributedFunction<E_RIGHT, K> rightKeyFn
    ) {
        return new JoinOn<>(leftKeyFn, rightKeyFn);
    }

    public DistributedFunction<E_LEFT, K> leftKeyFn() {
        return leftKeyFn;
    }

    public DistributedFunction<E_RIGHT, K> rightKeyFn() {
        return rightKeyFn;
    }
}
