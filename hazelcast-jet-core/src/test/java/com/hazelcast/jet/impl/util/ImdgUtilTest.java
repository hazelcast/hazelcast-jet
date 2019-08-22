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

package com.hazelcast.jet.impl.util;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class ImdgUtilTest extends JetTestSupport {

    @Test
    public void test_mapPutAllAsync_member() {
        JetInstance inst = createJetMember();
        createJetMember();

        IMap<Object, Object> map = inst.getHazelcastInstance().getMap("map");
        Map<String, String> tmpMap = new HashMap<>();
        tmpMap.put("k1", "v1");
        tmpMap.put("k2", "v1");
        ImdgUtil.mapPutAllAsync(map, tmpMap);

        assertEquals(tmpMap, new HashMap<>(map));
    }

    @Test
    public void test_mapPutAllAsync_client() {
        createJetMember();
        JetInstance client = createJetClient();

        IMap<Object, Object> map = client.getHazelcastInstance().getMap("map");
        Map<String, String> tmpMap = new HashMap<>();
        tmpMap.put("k1", "v1");
        tmpMap.put("k2", "v1");
        ImdgUtil.mapPutAllAsync(map, tmpMap);

        assertEquals(tmpMap, new HashMap<>(map));
    }
}
