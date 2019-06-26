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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.pipeline.Sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;

/**
 * Various sinks which can be used to assert incoming values. The sinks
 * can be used as terminal or inline in a pipeline using the convenience
 * provided in {@link Assertions}.
 *
 * @see Assertions
 * @since 3.2
 */
public final class AssertionSinks {

    private AssertionSinks() {
    }

    /**
     * Asserts that the previous stage emitted the exact sequence of expected
     * items, and nothing else. If the assertion fails, the job will fail with a
     * {@link AssertionError} with the given message.
     */
    public static <T> Sink<T> assertOrdered(String message, Collection<? super T> expected) {
        final List<? super T> exp = new ArrayList<>(expected);
        return assertCollected(received -> assertEquals(message, exp, received));
    }

    /**
     * Asserts that the previous stage emitted the expected
     * items in any order, but nothing else. If the assertion fails, the job
     * will fail with a {@link AssertionError} with the given message.
     */
    public static <T> Sink<T> assertUnordered(String message, Collection<? super T> expected) {
        final List<? super T> exp = new ArrayList<>(expected);
        exp.sort(hashCodeComparator());
        return assertCollected(received -> {
            received.sort(hashCodeComparator());
            assertEquals(message, exp, received);
        });
    }

    /**
     * Asserts that the previous stage emitted all of the given items in any order.
     * If the assertion fails, the job will fail with a {@link AssertionError} with
     * the given message.
     **/
    public static <T> Sink<T> assertContains(String message, Collection<? super T> expected) {
        final HashSet<? super T> set = new HashSet<>(expected);
        return AssertionSinkBuilder.assertionSink("assertContains", () -> set)
            .<T>receiveFn(HashSet::remove)
            .completeFn(exp -> {
                assertTrue(message + ", the following items have not been observed: " + exp, exp.isEmpty());
            })
            .build();
    }

    /**
     * Collects all the received items in a list and once the upstream stage
     * is completed it executes the assertion supplied by {@code assertFn}.
     **/
    public static <T> Sink<T> assertCollected(ConsumerEx<List<? super T>> assertFn) {
        return AssertionSinkBuilder.assertionSink("assertCollected", ArrayList::new)
            .<T>receiveFn(ArrayList::add)
            .completeFn(assertFn::accept)
            .build();
    }

    private static Comparator<Object> hashCodeComparator() {
        return Comparator.comparingInt(Object::hashCode);
    }

}
