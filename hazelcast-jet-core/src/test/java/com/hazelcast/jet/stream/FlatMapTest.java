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

import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static org.junit.Assert.assertEquals;

public class FlatMapTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        int repetitions = 10;

        IList<Integer> result = map.stream()
                                   .flatMap(e -> IntStream.iterate(e.getValue(), IntUnaryOperator.identity())
                                                          .limit(repetitions)
                                                          .boxed())
                                   .collect(DistributedCollectors.toIList(uniqueListName()));

        assertEquals(COUNT * repetitions, result.size());

        List<Integer> sortedResult = sortedList(result);
        for (int i = 0; i < COUNT; i++) {
            for (int j = i; j < repetitions; j++) {
                int val = sortedResult.get(i * repetitions + j);
                assertEquals(i, val);
            }
        }
    }

    @Test
    public void sourceList() {
        IStreamList<Integer> list = getList();
        fillList(list);

        int repetitions = 10;

        IList<Integer> result = list.stream()
                                    .flatMap(i -> IntStream.iterate(i, IntUnaryOperator.identity())
                                                           .limit(repetitions)
                                                           .boxed())
                                    .collect(DistributedCollectors.toIList(uniqueListName()));

        assertEquals(COUNT * repetitions, result.size());

        for (int i = 0; i < COUNT; i++) {
            for (int j = i; j < repetitions; j++) {
                int val = result.get(i * repetitions + j);
                assertEquals(i, val);
            }
        }
    }
}
