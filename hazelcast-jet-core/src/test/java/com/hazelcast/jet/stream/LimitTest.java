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

package com.hazelcast.jet.stream;

import com.hazelcast.core.IList;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static org.junit.Assert.assertEquals;

public class LimitTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        int limit = 10;
        IList list = map.stream()
                        .limit(limit)
                        .collect(DistributedCollectors.toIList(uniqueListName()));


        assertEquals(limit, list.size());
    }

    @Test
    public void intermediateOperation() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        int limit = 10;
        IList list = map.stream()
                        .map(Map.Entry::getValue)
                        .limit(limit)
                        .collect(DistributedCollectors.toIList(uniqueListName()));


        assertEquals(limit, list.size());
    }

    @Test
    public void sourceList() {
        IStreamList<Integer> list = getList();
        fillList(list);

        int limit = 10;
        IList<Integer> result = list.stream()
                                    .limit(limit)
                                    .collect(DistributedCollectors.toIList(uniqueListName()));

        assertEquals(limit, result.size());
    }
}
