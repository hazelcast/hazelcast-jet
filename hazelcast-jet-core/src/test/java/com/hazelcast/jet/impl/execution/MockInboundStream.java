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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.impl.util.ProgressState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.WAS_ALREADY_DONE;
import static java.util.Collections.emptyList;

public class MockInboundStream implements InboundEdgeStream {
    private final int chunkSize;
    private final List<Object> mockData;
    private final int ordinal;
    private int dataIndex;
    private boolean done;

    MockInboundStream(int ordinal, List<?> mockData, int chunkSize) {
        this.ordinal = ordinal;
        this.chunkSize = chunkSize;
        if (mockData.isEmpty()) {
            done = true;
            this.mockData = emptyList();
        } else {
            this.mockData = new ArrayList<>(mockData);
        }
    }

    void push(Object... items) {
        mockData.addAll(Arrays.asList(items));
    }

    @Override
    public ProgressState drainTo(Collection<Object> dest) {
        if (done || dataIndex == mockData.size()) {
            return WAS_ALREADY_DONE;
        }
        final int limit = Math.min(mockData.size(), dataIndex + chunkSize);
        for (; dataIndex < limit; dataIndex++) {
            final Object item = mockData.get(dataIndex);
            if (item == DONE_ITEM) {
                done = true;
            } else {
                dest.add(item);
            }
        }
        return done ? DONE : MADE_PROGRESS;
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return 0;
    }
}
