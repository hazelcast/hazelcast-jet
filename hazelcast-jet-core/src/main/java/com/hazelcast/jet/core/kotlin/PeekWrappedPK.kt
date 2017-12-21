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

import com.hazelcast.instance.HazelcastInstanceImpl
import com.hazelcast.jet.Util.entry
import com.hazelcast.jet.core.Inbox
import com.hazelcast.jet.core.Outbox
import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.function.DistributedFunction
import com.hazelcast.jet.impl.execution.init.Contexts
import com.hazelcast.jet.impl.execution.init.ExecutionPlan.createLoggerName
import com.hazelcast.logging.ILogger
import java.util.*
import java.util.function.Predicate
import java.util.stream.IntStream
import kotlin.coroutines.experimental.Continuation

fun <T> peekWrappedPK(
        wrapped: ProcessorK,
        toStringFn: DistributedFunction<T, String>,
        shouldLogFn: Predicate<T>,
        peekInput: Boolean,
        peekOutput: Boolean,
        peekSnapshot: Boolean
) = KotlinWrapperP(PeekWrappedPK(wrapped, toStringFn, shouldLogFn, peekInput, peekOutput, peekSnapshot))

class PeekWrappedPK<in T>(
        private val wrapped: ProcessorK,
        private val toStringFn: DistributedFunction<T, String>,
        private val shouldLogFn: Predicate<T>,
        peekInput: Boolean,
        private val peekOutput: Boolean,
        private val peekSnapshot: Boolean
): ProcessorK {
    override lateinit var suspendAction: (Continuation<Any>) -> Unit
    override var isCooperative
        get() = wrapped.isCooperative
        set(value) { wrapped.isCooperative = value }

    private lateinit var logger: ILogger
    private lateinit var outbox: Outbox
    private val loggingInbox: LoggingInbox? = if (peekInput) LoggingInbox() else null

    override fun init(outbox: Outbox, context: Processor.Context) {
        this.logger = context.logger()
        this.outbox = if (peekOutput || peekSnapshot)
            LoggingOutbox(outbox, peekOutput, peekSnapshot)
            else outbox

        // Fix issue #595: pass a logger with real class name to processor
        // We do this only if context is ProcCtx (that is, not for tests where TestProcessorContext can be used
        // and also other objects could be mocked or null, such as jetInstance())
        @Suppress("NAME_SHADOWING")
        val context = if (context is Contexts.ProcCtx) {
            val nodeEngine = (context.jetInstance().hazelcastInstance as HazelcastInstanceImpl).node.nodeEngine
            val newLogger = nodeEngine.getLogger(createLoggerName(
                    wrapped.javaClass.name, context.vertexName(), context.globalProcessorIndex()))
            Contexts.ProcCtx(context.jetInstance(), context.serializationService, newLogger, context.vertexName(),
                    context.globalProcessorIndex(), context.processingGuarantee())
        } else context
        wrapped.init(outbox, context)
    }

    internal fun log(prefix: String, t: T?) {
        // null object can come from poll()
        if (t != null && shouldLogFn.test(t)) {
            logger.info(prefix + ": " + toStringFn.apply(t))
        }
    }

    override suspend fun process(ordinal: Int, inbox: Inbox) {
        if (loggingInbox != null) {
            loggingInbox.wrappedInbox = inbox
            loggingInbox.ordinal = ordinal
            wrapped.process(ordinal, loggingInbox)
        } else {
            wrapped.process(ordinal, inbox)
        }
    }

    override suspend fun process() = wrapped.process()

    override suspend fun completeEdge() = wrapped.completeEdge()

    override suspend fun complete() = wrapped.complete()

    override suspend fun saveToSnapshot() = wrapped.saveToSnapshot()

    override suspend fun restoreFromSnapshot(inbox: Inbox) = wrapped.restoreFromSnapshot(inbox)

    override suspend fun finishSnapshotRestore() = wrapped.finishSnapshotRestore()

    private inner class LoggingInbox : Inbox {
        internal var ordinal: Int = 0
        internal lateinit var wrappedInbox: Inbox
        private var peekedItemLogged: Boolean = false

        override fun isEmpty(): Boolean {
            return wrappedInbox.isEmpty
        }

        override fun peek(): Any? {
            @Suppress("UNCHECKED_CAST")
            val res = wrappedInbox.peek() as T
            if (!peekedItemLogged && res != null) {
                log(res)
                peekedItemLogged = true
            }
            return res
        }

        override fun poll(): Any? {
            @Suppress("UNCHECKED_CAST")
            val res = wrappedInbox.poll() as T
            if (!peekedItemLogged && res != null) {
                log(res)
            }
            peekedItemLogged = false
            return res
        }

        override fun remove(): Any {
            peekedItemLogged = false
            return wrappedInbox.remove()
        }

        private fun log(res: T) {
            log("Input from " + ordinal, res)
        }
    }

    private inner class LoggingOutbox(
            private val wrappedOutbox: Outbox,
            private val logOutput: Boolean,
            private val logSnapshot: Boolean
    ) : Outbox {
        private val all: IntArray = IntStream.range(0, wrappedOutbox.bucketCount()).toArray()
        private val broadcastTracker: BitSet = BitSet(wrappedOutbox.bucketCount())

        override fun bucketCount(): Int {
            return wrappedOutbox.bucketCount()
        }

        override fun offer(ordinal: Int, item: Any): Boolean {
            if (ordinal == -1) {
                return offer(all, item)
            }
            if (!wrappedOutbox.offer(ordinal, item)) {
                return false
            }
            if (logOutput) {
                @Suppress("UNCHECKED_CAST")
                log("Output to " + ordinal, item as T)
            }
            return true
        }

        override fun offer(ordinals: IntArray, item: Any): Boolean {
            // use broadcast logic to be able to report accurately
            // which queue was pushed to when.
            var done = true
            for (i in ordinals.indices) {
                if (broadcastTracker.get(i)) {
                    continue
                }
                if (offer(i, item)) {
                    broadcastTracker.set(i)
                } else {
                    done = false
                }
            }
            if (done) {
                broadcastTracker.clear()
            }
            return done
        }

        override fun offerToSnapshot(key: Any, value: Any): Boolean {
            if (!wrappedOutbox.offerToSnapshot(key, value)) {
                return false
            }
            if (logSnapshot) {
                @Suppress("UNCHECKED_CAST")
                log("Output to snapshot", entry<Any, Any>(key, value) as T)
            }
            return true
        }
    }
}
