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
import java.util.List;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;


public final class TestSinks {

    private TestSinks() {

    }

    public static <T> Sink<T> equals(String message, List<T> expected) {
        return AssertionSinkBuilder.assertionSink("equals", ArrayList::new)
            .<T>receiveFn(ArrayList::add)
            .completeFn(received -> assertEquals(message, expected, received))
            .build();
    }


}
