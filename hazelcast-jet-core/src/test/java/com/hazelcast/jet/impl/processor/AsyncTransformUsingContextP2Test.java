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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;

@RunWith(HazelcastSerialClassRunner.class)
public class AsyncTransformUsingContextP2Test {

    @Test
    public void test_completedFutures() {
        TestSupport
                .verifyProcessor(AsyncTransformUsingContextP2.<String, String, String, String>supplier(
                        ContextFactory.withCreateFn(jet -> "foo"),
                        (ctx, item) -> completedFuture(traverseItems(item + "-1", item + "-2")),
                        10,
                        t -> "key"))
                .input(asList("a", "b"))
                .expectOutput(asList("a-1", "a-2", "b-1", "b-2"));
    }

    @Test
    public void test_futuresCompletedInSeparateThread() {
        TestSupport
                .verifyProcessor(AsyncTransformUsingContextP2.<String, String, String, String>supplier(
                        ContextFactory.withCreateFn(jet -> "foo"),
                        (ctx, item) -> {
                            CompletableFuture<Traverser<String>> f = new CompletableFuture<>();
                            spawn(() -> f.complete(traverseItems(item + "-1", item + "-2")));
                            return f;
                        },
                        10,
                        t -> "key"))
                .input(asList("a", "b", new Watermark(10)))
                .outputChecker((expected, actual) ->
                        actual.equals(asList("a-1", "a-2", "b-1", "b-2", wm(10)))
                                || actual.equals(asList("b-1", "b-2", "a-1", "a-2", wm(10))))
                .disableProgressAssertion()
                .expectOutput(singletonList("<see code>"));
    }

    @Test
    public void test_mapToNull() {
        TestSupport
                .verifyProcessor(AsyncTransformUsingContextP2.<String, String, String, String>supplier(
                        ContextFactory.withCreateFn(jet -> "foo"),
                        (ctx, item) -> null,
                        10,
                        t -> "key"))
                .input(asList("a", "b"))
                .expectOutput(emptyList());
    }

    @Test
    public void test_forwardWatermarksWithoutItems() {
        TestSupport
                .verifyProcessor(AsyncTransformUsingContextP2.<String, String, String, String>supplier(
                        ContextFactory.withCreateFn(jet -> "foo"),
                        (ctx, item) -> { throw new UnsupportedOperationException(); },
                        10,
                        t -> "key"))
                .input(singletonList(wm(10)))
                .expectOutput(singletonList(wm(10)));
    }
}
