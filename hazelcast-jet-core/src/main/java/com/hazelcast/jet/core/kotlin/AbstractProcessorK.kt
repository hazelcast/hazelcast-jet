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

import com.hazelcast.jet.core.Inbox
import com.hazelcast.jet.core.Outbox
import com.hazelcast.jet.core.Processor
import com.hazelcast.logging.ILogger
import kotlin.collections.Map.Entry
import kotlin.coroutines.experimental.Continuation

abstract class AbstractProcessorK : ProcessorK {
    override lateinit var suspendAction: (Continuation<Any>) -> Unit
    protected lateinit var logger: ILogger
    protected lateinit var outbox: Outbox

    override final fun init(outbox: Outbox, context: Processor.Context) {
        this.outbox = outbox
        this.logger = context.logger()
        init(context)
    }

    override final suspend fun process(ordinal: Int, inbox: Inbox) {
        when (ordinal) {
            0 -> process0(inbox)
            1 -> process1(inbox)
            2 -> process2(inbox)
            3 -> process3(inbox)
            4 -> process4(inbox)
            else -> processAny(ordinal, inbox)
        }
    }

    override final suspend fun restoreFromSnapshot(inbox: Inbox) {
        while (true) {
            val (key, value) = inbox.poll() as Entry<*, *>? ?: return
            restoreFromSnapshot(key!!, value!!)
        }
    }

    protected open fun init(context: Processor.Context) = Unit

    protected suspend fun emit(item: Any) {
        while (!outbox.offer(item)) {
            yield()
        }
    }

    protected suspend fun emitToSnapshot(key: Any, value: Any) {
        while (!outbox.offerToSnapshot(key, value)) {
            yield()
        }
    }

    protected open suspend fun process0(item: Any) = process(0, item)

    protected open suspend fun process1(item: Any) = process(1, item)

    protected open suspend fun process2(item: Any) = process(2, item)

    protected open suspend fun process3(item: Any) = process(3, item)

    protected open suspend fun process4(item: Any) = process(4, item)

    protected open suspend fun process(ordinal: Int, item: Any) = Unit

    protected open suspend fun restoreFromSnapshot(key: Any, value: Any) = Unit



    // The processN methods contain repeated looping code in order to give an
    // easier job to the JIT compiler to optimize each case independently, and
    // to ensure that ordinal is dispatched on just once per process(ordinal,
    // inbox) call.
    // An implementation with a very low-cost tryProcessN() method may want
    // to override processN() with an identical method, but which the JIT
    // compiler will be able to independently optimize and avoid the cost
    // of the megamorphic call site of tryProcessN here.

    protected open suspend fun process0(inbox: Inbox) {
        while (true) {
            val item = inbox.peek() ?: return
            process0(item)
            inbox.remove()
        }
    }

    protected open suspend fun process1(inbox: Inbox) {
        while (true) {
            val item = inbox.peek() ?: return
            process1(item)
            inbox.remove()
        }
    }

    protected open suspend fun process2(inbox: Inbox) {
        while (true) {
            val item = inbox.peek() ?: return
            process2(item)
            inbox.remove()
        }
    }

    protected open suspend fun process3(inbox: Inbox) {
        while (true) {
            val item = inbox.peek() ?: return
            process3(item)
            inbox.remove()
        }
    }

    protected open suspend fun process4(inbox: Inbox) {
        while (true) {
            val item = inbox.peek() ?: return
            process4(item)
            inbox.remove()
        }
    }

    protected open suspend fun processAny(ordinal: Int, inbox: Inbox) {
        while (true) {
            val item = inbox.peek() ?: return
            process(ordinal, item)
            inbox.remove()
        }
    }
}
