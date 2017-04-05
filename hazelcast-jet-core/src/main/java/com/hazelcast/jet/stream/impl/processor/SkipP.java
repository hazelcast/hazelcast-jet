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

package com.hazelcast.jet.stream.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Traversers.ResettableSingletonTraverser;

import javax.annotation.Nonnull;

public class SkipP extends AbstractProcessor {

    private final long skipCount;
    private final FlatMapper<Object, Object> flatMapper = flatMapper(new ResettableSingletonTraverser<>()::reset);
    private long index;

    public SkipP(long skipCount) {
        this.skipCount = skipCount;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (index < skipCount) {
            index++;
            return true;
        }
        return flatMapper.tryProcess(item);
    }
}
