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

package com.hazelcast.jet.core.kotlin

import com.hazelcast.jet.Traverser
import com.hazelcast.jet.core.Inbox
import com.hazelcast.jet.core.Outbox
import com.hazelcast.jet.core.Processor
import com.hazelcast.logging.ILogger
import kotlin.collections.Map.Entry
import kotlin.coroutines.experimental.Continuation

@Suppress("NOTHING_TO_INLINE")
abstract class AbstractProcessorK : ProcessorK {
    override lateinit var suspendAction: (Continuation<Any>) -> Unit
    protected lateinit var logger: ILogger
    protected lateinit var outbox: Outbox

    final override fun init(outbox: Outbox, context: Processor.Context) {
        this.outbox = outbox
        this.logger = context.logger()
        init(context)
    }

    protected open fun init(context: Processor.Context) = Unit

    protected suspend inline fun emit(item: Any) {
        while (!outbox.offer(item)) {
            yield()
        }
    }

    protected suspend inline fun emitToSnapshot(key: Any, value: Any) {
        while (!outbox.offerToSnapshot(key, value)) {
            yield()
        }
    }
}

inline fun Inbox.drain(block: (Any) -> Unit) {
    while (true) {
        val item = peek() ?: return
        block(item)
        remove()
    }
}

inline fun Inbox.drainSnapshot(block: (key: Any, value: Any) -> Unit) {
    while (true) {
        val (key, value) = peek() as? Entry<*, *> ?: return
        block(key!!, value!!)
        remove()
    }
}

inline fun <T : Any> Traverser<T>.forEach(block: (T) -> Unit) {
    while (true) {
        val t = next() ?: return
        block(t)
    }
}
