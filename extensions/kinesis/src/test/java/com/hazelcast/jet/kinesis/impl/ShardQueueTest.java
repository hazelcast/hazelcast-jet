/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.model.Shard;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ShardQueueTest {

    @Test
    public void smokeTest() {
        ShardQueue q = new ShardQueue();
        assertNull(q.pollAdded());
        assertNull(q.pollExpired());

        Shard shard = new Shard();
        q.addAdded(shard);
        assertNull(q.pollExpired());
        assertSame(shard, q.pollAdded());
        assertNull(q.pollAdded());

        String shardId = "foo";
        q.addExpired(shardId);
        assertNull(q.pollAdded());
        assertSame(shardId, q.pollExpired());
        assertNull(q.pollExpired());
    }
}
