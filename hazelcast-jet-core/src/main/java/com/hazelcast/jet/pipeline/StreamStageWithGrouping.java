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

package com.hazelcast.jet.pipeline;

import javax.annotation.Nonnull;

/**
 * Represents an intermediate step while constructing a windowed
 * group-and-aggregate pipeline stage. It captures the grouping key
 * and offers a method to specify the window definition.
 * @param <T> type of the stream items
 * @param <K> type of the key
 */
public interface StreamStageWithGrouping<T, K> extends GeneralStageWithGrouping<T, K> {

    /**
     * Adds the definition of the window to use in the group-and-aggregate
     * pipeline stage being constructed.
     */
    @Nonnull
    StageWithGroupingAndWindow<T, K> window(@Nonnull WindowDefinition wDef);
}
