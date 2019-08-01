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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static java.util.Collections.emptyList;

@RunWith(HazelcastParallelClassRunner.class)
public class ReadMapPTest extends JetTestSupport {

    private JetInstance jet;

    @Before
    public void setUp() {
        jet = createJetMember();
    }

    @Test
    public void test() {
        IMap<Integer, String> map = jet.getMap("map");
        List<Entry<Integer, String>> expected = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            map.put(i, "value-" + i);
            expected.add(entry(i, "value-" + i));
        }

        TestSupport
            .verifyProcessor(ReadMapP.readMapSupplier("map", null, null))
            .jetInstance(jet)
            .disableSnapshots()
            .disableProgressAssertion()
            .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
            .expectOutput(expected);
    }

    @Test
    public void testEmpty() {
        TestSupport
            .verifyProcessor(ReadMapP.readMapSupplier("map", null, null))
            .jetInstance(jet)
            .disableSnapshots()
            .disableProgressAssertion()
            .expectOutput(emptyList());
    }
}
