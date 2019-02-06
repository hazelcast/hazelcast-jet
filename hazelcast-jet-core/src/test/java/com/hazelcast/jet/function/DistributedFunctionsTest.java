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

import com.google.common.base.Objects;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.function.DistributedFunctions.constantKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.function.DistributedPredicate.alwaysFalse;
import static com.hazelcast.jet.function.DistributedPredicate.alwaysTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
public class DistributedFunctionsTest extends HazelcastTestSupport {

    @Test
    public void constructor() {
        assertUtilityConstructor(DistributedFunctions.class);
    }

    @Test
    public void when_wholeItem() {
        Object o = new Object();
        assertSame(o, wholeItem().apply(o));
    }

    @Test
    public void when_entryKey() {
        assertEquals(1, entryKey().apply(entry(1, 2)));
    }

    @Test
    public void when_entryValue() {
        assertEquals(2, entryValue().apply(entry(1, 2)));
    }

    @Test
    public void when_constantKey() {
        DistributedFunction<Object, String> f = constantKey();
        assertEquals(f.apply(1), f.apply(2));
        assertEquals(f.apply(1), f.apply(3));
    }

    @Test
    public void when_constantKeyCalledTwice_then_constantKeyLikelyDifferent() {
        for (int i = 0; i < 10; i++) {
            if (!Objects.equal(constantKey().apply(1), constantKey().apply(1))) {
                return;
            }
        }
        fail("10 calls generated the same key, this is extremely unlikely");
    }

    @Test
    public void when_alwaysTrue() {
        assertEquals(true, alwaysTrue().test(3));
    }

    @Test
    public void when_alwaysFalse() {
        assertEquals(false, alwaysFalse().test(2));
    }

    @Test
    public void when_noopConsumer() {
        // assert it's non-null and doesn't fail
        DistributedConsumer.noop().accept(1);
    }
}
