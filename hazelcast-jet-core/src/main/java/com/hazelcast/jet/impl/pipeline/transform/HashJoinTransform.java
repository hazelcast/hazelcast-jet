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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.pipeline.JoinClause;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BiFunction;

public class HashJoinTransform<T0, R> extends AbstractTransform implements Transform {
    @Nonnull
    public final List<JoinClause<?, ? super T0, ?, ?>> clauses;
    @Nonnull
    public final List<Tag> tags;
    @Nullable
    public final BiFunction mapToOutputBiFn;
    @Nullable
    public final DistributedTriFunction mapToOutputTriFn;

    public HashJoinTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull List<JoinClause<?, ? super T0, ?, ?>> clauses,
            @Nonnull List<Tag> tags,
            @Nonnull DistributedBiFunction mapToOutpuBiFn
    ) {
        super(tags.size() + "-way hash-join", upstream);
        this.clauses = clauses;
        this.tags = tags;
        this.mapToOutputBiFn = mapToOutpuBiFn;
        this.mapToOutputTriFn = null;
    }

    public <T1, T2> HashJoinTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull List<JoinClause<?, ? super T0, ?, ?>> clauses,
            @Nonnull List<Tag> tags,
            @Nonnull DistributedTriFunction<T0, T1, T2, R> mapToOutputTriFn
    ) {
        super(tags.size() + "-way hash-join", upstream);
        this.clauses = clauses;
        this.tags = tags;
        this.mapToOutputBiFn = null;
        this.mapToOutputTriFn = mapToOutputTriFn;
    }
}
