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

import com.hazelcast.jet.pipeline.Sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;

/**
 * @since 3.2
 */
public final class AssertionSinks {

    private AssertionSinks() {
    }

    /**
     *
     * @param message
     * @param expected
     * @param <T>
     * @return
     */
    public static <T> Sink<T> assertOrdered(String message, Collection<? super T> expected) {
        final List<? super T> exp = new ArrayList<>(expected);
        return AssertionSinkBuilder.assertionSink("assertOrdered", ArrayList::new)
            .<T>receiveFn(ArrayList::add)
            .completeFn(received -> assertEquals(message, exp, received))
            .build();
    }

    /**
     *
     * @param expected
     * @param <T>
     * @return
     */
    public static <T> Sink<T> assertOrdered(Collection<? super T> expected) {
        return assertOrdered(null, expected);
    }

    /**
     *
     * @param message
     * @param expected
     * @param <T>
     * @return
     */
    public static <T> Sink<T> assertUnordered(String message, Collection<? super T> expected) {
        final List<? super T> exp = new ArrayList<>(expected);
        exp.sort(hashCodeComparator());
        return AssertionSinkBuilder.assertionSink("assertUnordered", ArrayList::new)
            .<T>receiveFn(ArrayList::add)
            .completeFn(received -> {
                received.sort(hashCodeComparator());
                assertEquals(message, exp, received);
            })
            .build();
    }

//    public static <T> Sink<T> assertReceivedEventually() {
//
//    };
    /**
     *
     * @param expected
     * @param <T>
     * @return
     */
    public static <T> Sink<T> assertUnordered(Collection<? super T> expected) {
        return assertOrdered(null, expected);
    }

    private static Comparator<Object> hashCodeComparator() {
        return Comparator.comparingInt(Object::hashCode);
    }

}
