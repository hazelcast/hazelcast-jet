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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.PipelineTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;

public class AssertionsTest extends PipelineTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test_assertOrdered() {
        p.drawFrom(TestSources.items(1, 2, 3, 4))
         .apply(Assertions.assertOrdered(Arrays.asList(1, 2, 3, 4)));

        execute();
    }

    @Test
    public void test_assertOrdered_should_fail() throws Throwable {
        p.drawFrom(TestSources.items(4, 3, 2, 1))
         .apply(Assertions.assertOrdered(Arrays.asList(1, 2, 3, 4)));

        expectedException.expect(AssertionError.class);
        executeAndPeel();
    }

    @Test
    public void test_assertUnordered() {
        p.drawFrom(TestSources.items(4, 3, 2, 1))
         .apply(Assertions.assertUnordered(Arrays.asList(1, 2, 3, 4)));

        execute();
    }

    @Test
    public void test_assertUnordered_should_fail() throws Throwable {
        p.drawFrom(TestSources.items(3, 2, 1))
         .apply(Assertions.assertUnordered(Arrays.asList(1, 2, 3, 4)));

        expectedException.expect(AssertionError.class);
        executeAndPeel();
    }

    private void executeAndPeel() throws Throwable {
        try {
            execute();
        } catch (CompletionException e) {
            Throwable t = peel(e);
            if (t instanceof JetException && t.getCause() != null) {
                t = t.getCause();
            }
            throw t;
        }
    }
}
