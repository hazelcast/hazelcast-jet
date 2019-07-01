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
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * Various sinks which can be used to assert incoming values. The sinks
 * can be used as terminal or inline in a pipeline using the convenience
 * provided in {@link Assertions}.
 *
 * @see Assertions
 * @since 3.2
 */
@Beta
public final class AssertionSinks {

    private AssertionSinks() {
    }

    /**
     * Asserts that the previous stage emitted the exact sequence of expected
     * items, and nothing else. If the assertion fails, the job will fail with a
     * {@link AssertionError} with the given message.
     */
    @Nonnull
    public static <T> Sink<T> assertOrdered(@Nullable String message, @Nonnull Collection<? extends T> expected) {
        final List<? super T> exp = new ArrayList<>(expected);
        return assertCollected(received -> assertEquals(message, exp, received));
    }

    /**
     * Asserts that the previous stage emitted the expected
     * items in any order, but nothing else. If the assertion fails, the job
     * will fail with a {@link AssertionError} with the given message.
     */
    @Nonnull
    public static <T> Sink<T> assertUnordered(@Nullable String message, @Nonnull Collection<? extends T> expected) {
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
    @Nonnull
    public static <T> Sink<T> assertContains(@Nullable String message, @Nonnull Collection<? extends T> expected) {
        final HashSet<? super T> set = new HashSet<>(expected);
        return AssertionSinkBuilder.assertionSink("assertContains", () -> set)
            .<T>receiveFn(HashSet::remove)
            .completeFn(exp -> assertTrue(
                    message + ", the following items have not been observed: " + exp,
                    exp.isEmpty()))
            .build();
    }

    /**
     * Collects all the received items in a list and once the upstream stage
     * is completed it executes the assertion supplied by {@code assertFn}.
     *
     * @param assertFn assertion to execute once all items are received
     **/
    @Nonnull
    public static <T> Sink<T> assertCollected(@Nonnull ConsumerEx<? super List<T>> assertFn) {
        return AssertionSinkBuilder.assertionSink("assertCollected", ArrayList<T>::new)
            .<T>receiveFn(ArrayList::add)
            .completeFn(assertFn)
            .build();
    }

    /**
     * Collects all the received items in a list and periodically runs assertFn.
     * {@link AssertionError} thrown from the {@code assertFn} will be ignored
     * until {@code timeoutSeconds} has passed, in which case it will be rethrown.
     * <p>
     * If {@code assertFn} completes with any other errors, the exception will be rethrown.
     * If {@code assertFn} completes without any error, the sink will throw an
     * {@link AssertionCompletedException}.
     *
     * @param timeoutSeconds timeout in seconds, after which any assertion error will be propagated
     * @param assertFn assertion to execute periodically
     **/
    @Nonnull
    public static <T> Sink<T> assertCollectedEventually(
            int timeoutSeconds,
            @Nonnull ConsumerEx<? super List<T>> assertFn
    ) {
        return AssertionSinkBuilder
                .assertionSink("assertCollectedEventually",
                        () -> new CollectingSinkWithTimer<>(assertFn, timeoutSeconds))
            .<T>receiveFn(CollectingSinkWithTimer::receive)
            .timerFn(CollectingSinkWithTimer::timer)
            .completeFn(CollectingSinkWithTimer::complete)
            .build();
    }

    private static Comparator<Object> hashCodeComparator() {
        return Comparator.comparingInt(Object::hashCode);
    }

    private static final class CollectingSinkWithTimer<T> {

        private final long start;
        private final List<T> collected;
        private ConsumerEx<? super List<T>> assertFn;
        private int timeoutSeconds;

        CollectingSinkWithTimer(ConsumerEx<? super List<T>> assertFn, int timeoutSeconds) {
            this.assertFn = assertFn;
            this.timeoutSeconds = timeoutSeconds;
            start = System.currentTimeMillis();
            collected = new ArrayList<>();
        }

        void receive(T item) {
            collected.add(item);
        }

        void timer() {
            AssertionError error;
            try {
                assertFn.accept(collected);
                throw new AssertionCompletedException("Assertion completed successfully");
            } catch (AssertionError e) {
                error = e;
            } catch (Exception e) {
                throw rethrow(e);
            }
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > TimeUnit.SECONDS.toMillis(timeoutSeconds)) {
                throw new AssertionError("The following assertion failed after " + timeoutSeconds + " seconds", error);
            }
        }

        void complete() {
            assertFn.accept(collected);
        }
    }
}
