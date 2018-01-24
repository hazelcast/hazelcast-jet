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

@file:Suppress("NOTHING_TO_INLINE")

package com.hazelcast.jet.impl.execution

import com.hazelcast.jet.JetException
import com.hazelcast.jet.core.Watermark
import com.hazelcast.jet.core.kotlin.ProcessorK
import com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM
import com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE
import com.hazelcast.jet.impl.execution.WatermarkCoalescer.NO_NEW_WM
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
import kotlin.coroutines.experimental.intrinsics.suspendCoroutineOrReturn

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
    private val watermarkCoalescer = WatermarkCoalescer.create(maxWatermarkRetainMillis, instreams.size)
    private val outbox = run {
        val outstreams = outstreamList.sortedBy { it.ordinal() }.toTypedArray()
        val collectors = arrayOfNulls<OutboundCollector>(outstreams.size + (ssCollector?.let { 1 } ?: 0))
        for (i in outstreams.indices) {
            collectors[i] = outstreams[i].collector
        }
        ssCollector?.also { collectors[outstreams.size] = it }
        OutboxImpl(collectors, ssCollector != null, progTracker,
                context.serializationService, OUTBOX_BATCH_SIZE)
    }

    private var nextSnapshotId = ssContext.lastSnapshotId() + 1

    private var completing = false

    override fun isCooperative(): Boolean {
        return processor.isCooperative
    }

    private var continuation: Continuation<Unit>? = null
    private var continuationAfterSnapshot: Continuation<Unit>? = null

    private val suspendAction: (Continuation<Unit>) -> Any = {
        continuation = it
        COROUTINE_SUSPENDED
    }

    private val done = object : Continuation<Unit> {
        override val context = EmptyCoroutineContext
        override fun resume(value: Unit) = Unit
        override fun resumeWithException(exception: Throwable) = Unit
    }

    override fun init() {
        val outProcessor = context.serializationService.managedContext.initialize(processor)
        assert(outProcessor === processor) { "Managed context returned a different processor instance" }
        processor.init(outbox, context)
        processor.suspendAction = suspendAction
    }

    override fun call(): ProgressState {
        progTracker.reset()
        progTracker.notDone()
        outbox.resetBatch()
        val currentContinuation = continuation
        if (currentContinuation == null) {
            launch(Unconfined) { driveProcessor() }
        } else {
            continuation = null
            if (completing && context.snapshottingEnabled() && continuationAfterSnapshot == null) {
                launchSnapshotIfRequired(currentContinuation)
            }
            if (continuationAfterSnapshot == null) {
                currentContinuation.resume(Unit)
            }
        }
        if (continuation == null) {
            continuation = done
            return if (currentContinuation === done)
                ProgressState.WAS_ALREADY_DONE else
                ProgressState.DONE
        }
        return progTracker.toProgressState()
    }

    private fun launchSnapshotIfRequired(currentContinuation: Continuation<Unit>) {
        val currSnapshotId = ssContext.lastSnapshotId()
        assert(currSnapshotId <= nextSnapshotId) {
            "Unexpected new snapshot id $currSnapshotId, current id $nextSnapshotId"
        }
        if (currSnapshotId != nextSnapshotId) {
            return
        }
        continuationAfterSnapshot = currentContinuation
        launch(Unconfined) {
            processor.saveToSnapshot()
            emitToEdgesAndSnapshot(SnapshotBarrier(nextSnapshotId++))
            continuation = continuationAfterSnapshot
            continuationAfterSnapshot = null
        }
    }

    private suspend fun driveProcessor() {
        val instreamGroupQueue = instreams
                .groupByTo(TreeMap(), { it.priority() })
                .map { CircularListCursor(it.value) }
                .toCollection(ArrayDeque())
        val receivedBarriers = BitSet(instreams.size)
        while (true) {
            val now = watermarkCoalescer.time
            handleWm(watermarkCoalescer.checkWmHistory(now))
            val instreamCursor = instreamGroupQueue.poll() ?: break
            var itersSinceYield = 0
            do {
                val instream = instreamCursor.value()
                val ordinal = instream.ordinal()
                val drainResult = instream.drainTo { inbox.add(it) }
                when (inbox.peekLast()) {
                    null -> { }
                    is Watermark -> {
                        val wmVal = (inbox.removeLast() as Watermark).timestamp()
                        handleWm(watermarkCoalescer.observeWm(now, ordinal, wmVal))
                    }
                    is SnapshotBarrier -> {
                        val barrier = inbox.removeLast() as SnapshotBarrier
                        ensureCorrectId(barrier, nextSnapshotId, ordinal)
                        receivedBarriers.set(ordinal)
                    }
                    !is BroadcastItem -> {
                        watermarkCoalescer.observeEvent(ordinal)
                    }
                }
                if (drainResult.isMadeProgress) {
                    if (instream.hasSnapshotData()) {
                        processor.restoreFromSnapshot(inbox)
                    } else {
                        processor.process(ordinal, inbox)
                    }
                    assert(inbox.isEmpty) { "Processor did not drain the inbox: $this" }
                }
                if (drainResult.isDone) {
                    handleWm(watermarkCoalescer.queueDone(ordinal))
                    processor.completeEdge(ordinal)
                    instreamCursor.remove()
                } else if (context.snapshottingEnabled()
                        && receivedBarriers.cardinality() > 0
                        && receivedBarriers.cardinality() == instreamCursor.listSize()
                ) {
                    processor.saveToSnapshot()
                    emitToEdgesAndSnapshot(SnapshotBarrier(nextSnapshotId++))
                    receivedBarriers.clear()
                }
                if (++itersSinceYield >= instreamCursor.listSize()) {
                    if (!instream.hasSnapshotData()) {
                        processor.process()
                    }
                    yield()
                    itersSinceYield = 0
                }
            } while (instreamCursor.advance())
        }
        completing = true
        processor.complete()
        emitToEdgesAndSnapshot(DONE_ITEM)
    }

    private suspend inline fun handleWm(wmVal: Long) {
        if (wmVal == NO_NEW_WM) {
            return
        }
        val wm = Watermark(wmVal)
        if (wm != IDLE_MESSAGE) {
            processor.processWatermark(wm)
        }
        emit(wm)
    }

    private fun ensureCorrectId(barrier: SnapshotBarrier, pendingSnapshotId: Long, ordinal: Int) {
        if (barrier.snapshotId() != pendingSnapshotId) {
            throw JetException("Unexpected snapshot barrier ${barrier.snapshotId()} from ordinal $ordinal," +
                    " expected $pendingSnapshotId")
        }
    }

    private fun InboundEdgeStream.hasSnapshotData() = priority() == Integer.MIN_VALUE

    private suspend inline fun emit(item: Any) {
        while (!outbox.offer(item)) {
            yield()
        }
    }

    private suspend inline fun emitToEdgesAndSnapshot(item: Any) {
        while (!outbox.offerToEdgesAndSnapshot(item)) {
            yield()
        }
    }

    private suspend inline fun yield() = suspendCoroutineOrReturn(suspendAction)

    override fun toString(): String {
        return "ProcessorTasklet{vertex=${context.vertexName()}, processor=$processor}"
    }
}
