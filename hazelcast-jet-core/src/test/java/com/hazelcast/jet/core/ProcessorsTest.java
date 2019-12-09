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

package com.hazelcast.jet.core;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.processor.Processors.aggregateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.combineByKeyP;
import static com.hazelcast.jet.core.processor.Processors.combineP;
import static com.hazelcast.jet.core.processor.Processors.filterP;
import static com.hazelcast.jet.core.processor.Processors.filterUsingServiceAsyncP;
import static com.hazelcast.jet.core.processor.Processors.filterUsingServiceP;
import static com.hazelcast.jet.core.processor.Processors.flatMapP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingServiceP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.Processors.mapUsingServiceAsyncP;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class ProcessorsTest {

    @Test
    public void map() {
        TestSupport
                .verifyProcessor(mapP(Object::toString))
                .input(singletonList(1))
                .expectOutput(singletonList("1"));
    }

    @Test
    public void mapUsingService() {
        TestSupport
                .verifyProcessor(Processors.mapUsingServiceP(
                        ServiceFactory.<int[]>withCreateFn(context -> new int[1])
                                      .withDestroyFn(context -> assertEquals(6, context[0])),
                        (int[] context, Integer item) -> context[0] += item))
                .disableSnapshots()
                .input(asList(1, 2, 3))
                .expectOutput(asList(1, 3, 6));
    }

    @Test
    public void mapUsingServiceAsync() {
        TestSupport
                .verifyProcessor(mapUsingServiceAsyncP(
                        ServiceFactory.<AtomicInteger>withCreateFn(context -> new AtomicInteger())
                                      .withDestroyFn(context -> assertEquals(6, context.get())),
                        t -> "k",
                        (AtomicInteger context, Integer item) -> supplyAsync(() -> {
                            sleepMillis(100);
                            context.addAndGet(item);
                            return item;
                        })))
                .disableSnapshots()
                .disableProgressAssertion()
                .input(asList(1, 2, 3))
                .expectOutput(asList(1, 2, 3));
    }

    @Test
    public void filteringWithMap() {
        TestSupport
                .verifyProcessor(mapP((Integer i) -> i > 1 ? i : null))
                .input(asList(1, 2))
                .expectOutput(singletonList(2));
    }

    @Test
    public void filteringWithMapUsingService() {
        TestSupport
                .verifyProcessor(Processors.mapUsingServiceP(
                        ServiceFactory.<int[]>withCreateFn(context -> new int[1])
                                      .withDestroyFn(context -> assertEquals(3, context[0])),
                        (int[] context, Integer item) -> {
                            try {
                                return context[0] % 2 == 0 ? item : null;
                            } finally {
                                context[0] = item;
                            }
                        }))
                .disableSnapshots()
                .input(asList(1, 2, 3))
                .expectOutput(asList(1, 3));
    }

    @Test
    public void filteringWithMapUsingServiceAsync() {
        TestSupport
                .verifyProcessor(mapUsingServiceAsyncP(
                        ServiceFactory.<int[]>withCreateFn(context -> new int[] {2})
                                      .withDestroyFn(context -> assertEquals(2, context[0])),
                        t -> "k",
                        (int[] context, Integer item) ->
                                supplyAsync(() -> item % context[0] != 0 ? item : null)))
                .disableSnapshots()
                .disableProgressAssertion()
                .input(asList(1, 2, 3))
                .expectOutput(asList(1, 3));
    }

    @Test
    public void filter() {
        TestSupport
                .verifyProcessor(filterP(o -> o.equals(1)))
                .input(asList(1, 2, 1, 2))
                .expectOutput(asList(1, 1));
    }

    @Test
    public void filterUsingService() {
        TestSupport
                .verifyProcessor(filterUsingServiceP(
                        ServiceFactory.<int[]>withCreateFn(context -> new int[1])
                                      .withDestroyFn(context -> assertEquals(2, context[0])),
                        (int[] context, Integer item) -> {
                            try {
                                // will pass if greater than the previous item
                                return item > context[0];
                            } finally {
                                context[0] = item;
                            }
                        }))
                .input(asList(1, 2, 1, 2))
                .disableSnapshots()
                .expectOutput(asList(1, 2, 2));
    }

    @Test
    public void filterUsingServiceAsync() {
        TestSupport
                .verifyProcessor(filterUsingServiceAsyncP(
                        ServiceFactory.<AtomicInteger>withCreateFn(context -> new AtomicInteger())
                                      .withDestroyFn(context -> assertEquals(4, context.get())),
                        t -> "k",
                        (AtomicInteger context, Integer item) -> CompletableFuture.supplyAsync(() -> {
                            context.incrementAndGet();
                            return item > 1;
                        })))
                .input(asList(1, 2, 1, 2))
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(asList(2, 2));
    }

    @Test
    public void flatMap() {
        TestSupport
                .verifyProcessor(flatMapP(o -> traverseIterable(asList(o + "a", o + "b"))))
                .input(singletonList(1))
                .expectOutput(asList("1a", "1b"));
    }

    @Test
    public void flatMapUsingService() {
        int[] array = {0};

        TestSupport
                .verifyProcessor(flatMapUsingServiceP(
                        ServiceFactory.<int[]>withCreateFn(serviceContext -> array)
                                      .withDestroyFn(c -> c[0] = 0),
                        (int[] ary, Integer item) -> traverseItems(item, ary[0] += item)))
                .disableSnapshots()
                .input(asList(1, 2, 3))
                .expectOutput(asList(1, 1, 2, 3, 3, 6));

        assertEquals(0, array[0]);
    }

    @Test
    public void aggregateByKey() {
        FunctionEx<Object, String> keyFn = Object::toString;
        TestSupport
                .verifyProcessor(aggregateByKeyP(singletonList(keyFn), aggregateToListAndString(), Util::entry))
                .disableSnapshots()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .input(asList(1, 1, 2, 2))
                .expectOutput(asList(
                        entry("1", "[1, 1]"),
                        entry("2", "[2, 2]")
                ));
    }

    @Test
    public void accumulateByKey() {
        FunctionEx<Object, String> keyFn = Object::toString;
        TestSupport
                .verifyProcessor(Processors.accumulateByKeyP(singletonList(keyFn), aggregateToListAndString()))
                .disableSnapshots()
                .input(asList(1, 1, 2, 2))
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .expectOutput(asList(
                        entry("1", asList(1, 1)),
                        entry("2", asList(2, 2))
                ));
    }

    @Test
    public void combineByKey() {
        TestSupport
                .verifyProcessor(combineByKeyP(aggregateToListAndString(), Util::entry))
                .disableSnapshots()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .input(asList(
                        entry("1", asList(1, 2)),
                        entry("1", asList(3, 4)),
                        entry("2", asList(5, 6)),
                        entry("2", asList(7, 8))
                ))
                .expectOutput(asList(
                        entry("1", "[1, 2, 3, 4]"),
                        entry("2", "[5, 6, 7, 8]")
                ));
    }

    @Test
    public void aggregate() {
        TestSupport
                .verifyProcessor(Processors.aggregateP(aggregateToListAndString()))
                .disableSnapshots()
                .input(asList(1, 2))
                .expectOutput(singletonList("[1, 2]"));
    }

    @Test
    public void accumulate() {
        TestSupport
                .verifyProcessor(Processors.accumulateP(aggregateToListAndString()))
                .disableSnapshots()
                .input(asList(1, 2))
                .expectOutput(singletonList(asList(1, 2)));
    }

    @Test
    public void combine() {
        TestSupport
                .verifyProcessor(combineP(aggregateToListAndString()))
                .disableSnapshots()
                .input(asList(
                        singletonList(1),
                        singletonList(2)
                ))
                .expectOutput(singletonList("[1, 2]"));
    }

    @Test
    public void noop() {
        TestSupport
                .verifyProcessor(noopP())
                .input(Stream.generate(() -> "a").limit(100).collect(toList()))
                .expectOutput(emptyList());
    }

    private static <T> AggregateOperation1<T, List<T>, String> aggregateToListAndString() {
        return AggregateOperation
                .<List<T>>withCreate(ArrayList::new)
                .<T>andAccumulate(List::add)
                .andCombine(List::addAll)
                .andExportFinish(Object::toString);
    }
}
