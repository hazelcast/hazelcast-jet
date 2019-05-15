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

package com.hazelcast.jet.pipeline;

import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
public class SourceBuilderTest {

    @Test(expected = IllegalArgumentException.class)
    public void when_onlyCreateFnSpecified_then_fail() {
        SourceBuilder
                .stream("name", ctx -> null)
                .createSnapshotFn(src -> null)
                .fillBufferFn((src, buffer) -> { })
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_onlyRestoreFnSpecified_then_fail() {
        SourceBuilder
                .stream("name", ctx -> null)
                .restoreSnapshotFn((src, states) -> { })
                .fillBufferFn((src, buffer) -> { })
                .build();
    }
}
