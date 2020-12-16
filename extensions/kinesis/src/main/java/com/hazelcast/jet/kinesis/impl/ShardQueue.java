/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.model.Shard;

import javax.annotation.Nullable;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class ShardQueue {

    private final Queue<Object> queue = new LinkedBlockingQueue<>();

    private Object lastPolledValue;

    public void added(Shard shard) {
        queue.offer(shard);
    }

    public void expired(String shardId) {
        queue.offer(shardId);
    }

    public void poll() {
        lastPolledValue = queue.poll();
    }

    public Shard getAdded() {
        return get(Shard.class);
    }

    public String getExpired() {
        return get(String.class);
    }

    @Nullable
    private <T> T get(Class<T> clazz) {
        return clazz.isInstance(lastPolledValue) ? (T) lastPolledValue : null;
    }
}
