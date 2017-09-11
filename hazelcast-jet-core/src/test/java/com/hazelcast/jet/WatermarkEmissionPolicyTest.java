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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.WatermarkEmissionPolicy.emitByMinStep;
import static com.hazelcast.jet.WindowDefinition.tumblingWindowDef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WatermarkEmissionPolicyTest {

    private WatermarkEmissionPolicy p;

    @Test
    public void when_wmIncreasing_then_throttleByMinStep() {
        p = emitByMinStep(2);
        assertWm(2, 0, 1);
        assertWm(2, 0, 2);
        assertWm(3, 0, 3);
        assertWm(4, 0, 4);
    }

    @Test
    public void when_wmIncreasing_then_throttleByFrame() {
        p = emitByFrame(tumblingWindowDef(3));
        assertWm(3, 0, 2);
        assertWm(3, 0, 3);
        assertWm(3, 0, 4);
    }

    private void assertWm(long expectedNextWm, long lastEmittedWm, long newWm) {
        assertTrue(expectedNextWm > lastEmittedWm);
        assertEquals(expectedNextWm, p.nextWatermark(lastEmittedWm, newWm));
    }
}
