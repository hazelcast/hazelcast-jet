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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.core.Outbox;

public interface OutboxInternal extends Outbox {

    /**
     * Resets the outbox so that it is available to receive another batch of
     * items after any {@code offer()} method previously returned {@code
     * false}.
     */
    void reset();

    /**
     * Blocks the outbox so that it only allows offering of the current
     * unfinished item. If there's no unfinished item, the outbox will reject
     * all {@code offer} calls, until {@link #unblock()} is called.
     */
    void block();

    /**
     * Reverses the {@link #block()} call.
     */
    void unblock();
}
