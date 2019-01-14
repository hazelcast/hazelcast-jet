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

package com.hazelcast.jet.function;

import com.hazelcast.jet.impl.util.ExceptionUtil;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link TriPredicate} which declares checked
 * exception.
 */
@FunctionalInterface
public interface DistributedTriPredicate<T, U, V> extends TriPredicate<T, U, V>, Serializable {

    /**
     * Exception-declaring version of {@link TriPredicate#test}.
     */
    boolean testEx(T t, U u, V v) throws Exception;

    @Override
    default boolean test(T t, U u, V v) {
        try {
            return testEx(t, u, v);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * Returns a composite predicate which evaluates the
     * equivalent of {@code this.test(t, u, v) && other.test(t, u, v)}.
     *
     */
    default DistributedTriPredicate<T, U, V> and(
            @Nonnull DistributedTriPredicate<? super T, ? super U, ? super V> other
    ) {
        checkNotNull(other, "other");
        return (t, u, v) -> test(t, u, v) && other.test(t, u, v);
    }

    /**
     * Returns a composite predicate which evaluates the
     * equivalent of {@code !this.test(t, u, v)}.
     */
    default DistributedTriPredicate<T, U, V> negate() {
        return (t, u, v) -> !test(t, u, v);
    }

    /**
     * Returns a composite predicate which evaluates the
     * equivalent of {@code this.test(t, u, v) || other.test(t, u, v)}.
     */
    default DistributedTriPredicate<T, U, V> or(
            @Nonnull DistributedTriPredicate<? super T, ? super U, ? super V> other
    ) {
        checkNotNull(other, "other");
        return (t, u, v) -> test(t, u, v) || other.test(t, u, v);
    }
}
