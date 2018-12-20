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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.WatermarkSourceUtil;

import javax.annotation.Nonnull;

public class WatermarkSourceUtilNoWatermarksImpl<T> implements WatermarkSourceUtil<T> {

    private final ResettableSingletonTraverser<Object> traverser = new ResettableSingletonTraverser<>();

    @Nonnull
    @Override
    public Traverser<Object> handleEvent(T event, int partitionIndex, long nativeEventTime) {
        traverser.accept(event);
        return traverser;
    }

    @Nonnull
    @Override
    public Traverser<Object> handleNoEvent() {
        return Traversers.empty();
    }

    @Override
    public void increasePartitionCount(int newPartitionCount) {
    }

    @Override
    public long getWatermark(int partitionIndex) {
        return Long.MIN_VALUE;
    }

    @Override
    public void restoreWatermark(int partitionIndex, long wm) {
    }
}
