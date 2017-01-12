/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayDeque;

import static com.hazelcast.jet.Processors.map;
import static org.junit.Assert.*;

@Category(QuickTest.class)
public class ProcessorsTest {
    private TestInbox inbox;

    @Before
    public void before() {
        inbox = new TestInbox();
    }

    @Test
    public void test1() {
        final ProcessorSupplier sup = map(Object::toString);
        final Processor p = sup.get(1).iterator().next();
        final ArrayDequeOutbox outbox = new ArrayDequeOutbox(1, new int[]{1});
        p.init(outbox);
        inbox.offer(1);
        p.process(0, inbox);
        final Object emitted = outbox.queueWithOrdinal(0).remove();
        assertEquals("1", emitted);
    }

    private static class TestInbox extends ArrayDeque<Object> implements Inbox {
    }
}
