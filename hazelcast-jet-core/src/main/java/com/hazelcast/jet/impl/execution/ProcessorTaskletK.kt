/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution

import com.hazelcast.jet.core.kotlin.ProcessorK
import com.hazelcast.jet.core.kotlin.yield
import com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx
import com.hazelcast.jet.impl.util.ArrayDequeInbox
import com.hazelcast.jet.impl.util.CircularListCursor
import com.hazelcast.jet.impl.util.ProgressState
import com.hazelcast.jet.impl.util.ProgressTracker
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.launch
import java.util.*
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.EmptyCoroutineContext
import kotlin.coroutines.experimental.intrinsics.COROUTINE_SUSPENDED

private const val OUTBOX_BATCH_SIZE = 2048

class ProcessorTaskletK(
        private val context: ProcCtx,
        private val processor: ProcessorK,
        private val instreams: List<InboundEdgeStream>,
        outstreamList: List<OutboundEdgeStream>,
        private val ssContext: SnapshotContext,
        ssCollector: OutboundCollector?,
        maxWatermarkRetainMillis: Int
) : Tasklet {
    private val progTracker = ProgressTracker()
    private val inbox = ArrayDequeInbox(progTracker)
    private val outbox = createOutbox(outstreamList, ssCollector)

    private fun createOutbox(outstreamList: List<OutboundEdgeStream>, ssCollector: OutboundCollector?): OutboxImpl {
        val outstreams = outstreamList.sortedBy { it.ordinal() }.toTypedArray()
        val collectors = arrayOfNulls<OutboundCollector>(outstreams.size + (ssCollector?.let { 1 } ?: 0))
        for (i in outstreams.indices) {
            collectors[i] = outstreams[i].collector
        }
        ssCollector?.also {
            collectors[outstreams.size] = it
        }
        return OutboxImpl(collectors, ssCollector != null, progTracker,
                context.serializationService, OUTBOX_BATCH_SIZE)
    }

    override fun isCooperative(): Boolean {
        return processor.isCooperative
    }

    private var continuation: Continuation<Unit>? = null

    private val done = object : Continuation<Unit> {
        override val context = EmptyCoroutineContext
        override fun resume(value: Unit) = Unit
        override fun resumeWithException(exception: Throwable) = Unit
    }

    override fun init() {
        val processor2 = context.serializationService.managedContext.initialize(processor)
        assert(processor2 === processor) { "different object returned" }
        processor.init(outbox, context)
        processor.suspendAction = {
            continuation = it
            COROUTINE_SUSPENDED
        }
    }

    override fun call(): ProgressState {
        progTracker.reset()
        progTracker.notDone()
        outbox.resetBatch()
        val currentContinuation = continuation
        if (currentContinuation == null) {
            println("$processor launchProcessing")
            launchProcessing()
            println("after $processor launchProcessing")
        } else {
            continuation = null
            currentContinuation.resume(Unit)
        }
        if (continuation == null) {
            continuation = done
            println("$processor done")
            return if (currentContinuation == done)
                ProgressState.WAS_ALREADY_DONE else
                ProgressState.DONE
        }
        return progTracker.toProgressState()
    }

    private fun launchProcessing() = launch(Unconfined) coroutine@ {
        val instreamGroupQueue = instreams
                .groupByTo(TreeMap(), { it.priority() })
                .map { CircularListCursor(it.value) }
                .toCollection(ArrayDeque())
        while (true) {
            val instreamCursor = instreamGroupQueue.poll() ?: break
            do {
                val instream = instreamCursor.value()
                val result = instream.drainTo { inbox.add(it) }
                if (result.isMadeProgress) {
                    progTracker.madeProgress()
                    processor.process(instreamCursor.value().ordinal(), inbox)
                }
                if (result.isDone) {
                    processor.completeEdge(instream.ordinal())
                    instreamCursor.remove()
                }
                processor.yield()
            } while (instreamCursor.advance())
        }
        processor.complete()
        while (!outbox.offerToEdgesAndSnapshot(DONE_ITEM)) {
            processor.yield()
        }
    }

    override fun toString(): String {
        return "ProcessorTasklet{vertex=" + context.vertexName() + ", processor=" + processor + '}'.toString()
    }
}
