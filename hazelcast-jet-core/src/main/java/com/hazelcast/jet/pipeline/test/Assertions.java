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
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Various assertions which can be used to test pipelines for correctness
 * of output. Each assertion also returns the stage it is attached to so
 * the assertions could be used inline in a pipeline.
 *
 * @since 3.2
 */
@Beta
public final class Assertions {

    private Assertions() {

    }

    /**
     * Asserts that the previous stage emitted the exact sequence of expected
     * items and nothing else. If the assertion fails, the job will fail with a
     * {@link AssertionError} with the given message.
     * <p>
     * Example:
     * <pre>{@code
     * p.drawFrom(TestSources.items(1, 2, 3, 4))
     *  .apply(Assertions.assertOrdered("unexpected values", Arrays.asList(1, 2, 3, 4)))
     *  .drainTo(Sinks.logger())
     * }</pre>
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertOrdered(
        @Nullable String message, @Nonnull Collection<? super T> expected
    ) {
        return stage -> {
            stage.drainTo(AssertionSinks.assertOrdered(message, expected));
            return stage;
        };
    }

    /**
     * Asserts that the previous stage emitted the exact sequence of expected
     * items and nothing else. If the assertion fails, the job will fail with a
     * {@link AssertionError}.
     * Example:
     * <pre>{@code
     * p.drawFrom(TestSources.items(1, 2, 3, 4))
     *  .apply(Assertions.assertOrderedArrays.asList(1, 2, 3, 4)))
     *  .drainTo(Sinks.logger())
     * }</pre>
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertOrdered(
        @Nonnull Collection<? super T> expected
    ) {
        return assertOrdered(null, expected);
    }

    /**
     * Asserts that the previous stage emitted the expected
     * items in any order, but nothing else. If the assertion fails, the job will fail with a
     * {@link AssertionError} with the given message.
     * <p>
     * Example:
     * <pre>{@code
     * p.drawFrom(TestSources.items(4, 3, 2, 1))
     *  .apply(Assertions.assertUnordered("unexpected values", Arrays.asList(1, 2, 3, 4)))
     *  .drainTo(Sinks.logger())
     * }</pre>
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertUnordered(
        @Nullable String message, @Nonnull Collection<? super T> expected
    ) {
        return stage -> {
            stage.drainTo(AssertionSinks.assertUnordered(message, expected));
            return stage;
        };
    }

    /**
     * Asserts that the previous stage emitted the expected
     * items in any order, but nothing else. If the assertion fails, the job will fail with a
     * {@link AssertionError}.
     * <p>
     * Example:
     * <pre>{@code
     * p.drawFrom(TestSources.items(4, 3, 2, 1))
     *  .apply(Assertions.assertUnordered(Arrays.asList(1, 2, 3, 4)))
     *  .drainTo(Sinks.logger())
     * }</pre>
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertUnordered(
        @Nonnull Collection<? super T> expected
    ) {
        return assertUnordered(null, expected);
    }

    /**
     * Asserts that the previous stage emitted all of the given items in any order.
     * If the assertion fails, the job will fail with a {@link AssertionError} with
     * the given message.
     * <p>
     * Example:
     * <pre>{@code
     * p.drawFrom(TestSources.items(4, 3, 2, 1))
     *  .apply(Assertions.assertUnordered(Arrays.asList(1, 3)))
     *  .drainTo(Sinks.logger())
     * }</pre>
     **/
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertContains(
        @Nullable String message, @Nonnull Collection<? super T> expected
    ) {
        return stage -> {
            stage.drainTo(AssertionSinks.assertContains(message, expected));
            return stage;
        };
    }

    /**
     * Collects all the received items in a list and once the upstream stage
     * is completed it executes the assertion supplied by {@code assertFn}.
     * <p>
     * Example:
     * <pre>{@code
     * p.drawFrom(TestSources.items(1, 2, 3, 4))
     *  .apply(Assertions.assertCollected(items -> assertTrue("expected minimum of 4 items", items.size >= 4)))
     *  .drainTo(Sinks.logger())
     * }</pre>
     **/
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertCollected(
        @Nonnull ConsumerEx<List<? super T>> assertFn
    ) {
        return stage -> {
            stage.drainTo(AssertionSinks.assertCollected(assertFn));
            return stage;
        };
    }

    /**
     * Collects all the received items in a list and periodically runs assertFn.
     * {@link AssertionError} thrown from the {@code assertFn} will be ignored
     * until {@code timeoutSeconds} has passed, in which case they will be rethrown.
     * <p>
     * If {@code assertFn} completes with any other errors, the exception will be rethrown.
     * If {@code assertFn} completes without any error, the sink will throw an
     * {@link AssertionCompletedException}
     * Example:
     * <pre>{@code
     * p.drawFrom(TestSources.itemStream(10))
     * .withoutTimestamps()
     * .apply(assertCollectedEventually(5, c -> assertTrue("did not receive at least 20 items", c.size() > 20)));
     * }</pre>
     **/
    @Nonnull
    public static <T> FunctionEx<StreamStage<T>, StreamStage<T>> assertCollectedEventually(
        int timeout, @Nonnull ConsumerEx<List<? super T>> assertFn
    ) {
        return stage -> {
            stage.drainTo(AssertionSinks.assertCollectedEventually(timeout, assertFn));
            return stage;
        };
    }
}
