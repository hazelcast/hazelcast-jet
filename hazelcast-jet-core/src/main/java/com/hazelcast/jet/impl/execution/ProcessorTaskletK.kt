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

import com.hazelcast.jet.JetException
import com.hazelcast.jet.config.ProcessingGuarantee
import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.core.Watermark
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx
import com.hazelcast.jet.impl.util.ArrayDequeInbox
import com.hazelcast.jet.impl.util.CircularListCursor
import com.hazelcast.jet.impl.util.ProgressState
import com.hazelcast.jet.impl.util.ProgressTracker
import com.hazelcast.util.Preconditions
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.BitSet
import kotlin.collections.Map.Entry
import java.util.Optional
import java.util.Queue
import java.util.TreeMap

import com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM
import com.hazelcast.jet.impl.execution.ProcessorState.COMPLETE
import com.hazelcast.jet.impl.execution.ProcessorState.COMPLETE_EDGE
import com.hazelcast.jet.impl.execution.ProcessorState.EMIT_BARRIER
import com.hazelcast.jet.impl.execution.ProcessorState.EMIT_DONE_ITEM
import com.hazelcast.jet.impl.execution.ProcessorState.EMIT_WATERMARK
import com.hazelcast.jet.impl.execution.ProcessorState.END
import com.hazelcast.jet.impl.execution.ProcessorState.PROCESS_INBOX
import com.hazelcast.jet.impl.execution.ProcessorState.PROCESS_WATERMARK
import com.hazelcast.jet.impl.execution.ProcessorState.SAVE_SNAPSHOT
import com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS
import java.util.Comparator.comparing
import java.util.function.Supplier
import java.util.stream.Collectors.groupingBy
import java.util.stream.Collectors.toCollection

private const val OUTBOX_BATCH_SIZE = 2048

class ProcessorTaskletK(private val context: ProcCtx,
                        private val processor: Processor,
                        instreams: List<InboundEdgeStream>,
                        outstreams: List<OutboundEdgeStream>,
                        private val ssContext: SnapshotContext,
                        ssCollector: OutboundCollector,
                        maxWatermarkRetainMillis: Int) : Tasklet {
    private val progTracker = ProgressTracker()
    private val outstreams: Array<OutboundEdgeStream>
    private val outbox: OutboxImpl
    private val receivedBarriers: BitSet // indicates if current snapshot is received on the ordinal

    private val inbox = ArrayDequeInbox(progTracker)
    private val instreamGroupQueue: Queue<out List<InboundEdgeStream>>
    private val watermarkCoalescer: WatermarkCoalescer

    private var numActiveOrdinals: Int = 0 // counter for remaining active ordinals
    private var instreamCursor: CircularListCursor<InboundEdgeStream>? = null
    private var currInstream: InboundEdgeStream? = null
    private var state: ProcessorState? = null
    private var pendingSnapshotId: Long = 0
    private var pendingWatermark: Watermark? = null

    /**
     * Tells whether the inbox we are currently on is the snapshot restoring inbox.
     */
    private fun isSnapshotInbox() = currInstream != null && currInstream!!.priority() == Integer.MIN_VALUE

    init {
        Preconditions.checkNotNull(processor, "processor")
        this.numActiveOrdinals = instreams.size
        this.instreamGroupQueue = instreams
                .groupByTo(TreeMap(), { it.priority() })
                .map { it.value }
                .mapTo(ArrayDeque(), { it })
        this.outstreams = outstreams.sortedBy { it.ordinal() }.toTypedArray()
        instreamCursor = popInstreamGroup()
        currInstream = instreamCursor?.value()
        outbox = createOutbox(ssCollector)
        receivedBarriers = BitSet(instreams.size)
        state = initialProcessingState()
        pendingSnapshotId = ssContext.lastSnapshotId() + 1

        watermarkCoalescer = WatermarkCoalescer.create(maxWatermarkRetainMillis, instreams.size)
    }

    private fun createOutbox(ssCollector: OutboundCollector?): OutboxImpl {
        val collectors = arrayOfNulls<OutboundCollector>(outstreams.size + if (ssCollector == null) 0 else 1)
        for (i in outstreams.indices) {
            collectors[i] = outstreams[i].collector
        }
        if (ssCollector != null) {
            collectors[outstreams.size] = ssCollector
        }
        return OutboxImpl(collectors, ssCollector != null, progTracker,
                context.serializationService, OUTBOX_BATCH_SIZE)
    }

    override fun init() {
        context.serializationService.managedContext.initialize(processor)
        processor.init(outbox, context)
    }

    override fun call(): ProgressState {
        return call(watermarkCoalescer.time)
    }

    // package-visible for testing
    internal fun call(now: Long): ProgressState {
        progTracker.reset()
        outbox.resetBatch()
        stateMachineStep(now)
        return progTracker.toProgressState()
    }

    private fun stateMachineStep(now: Long) {
        when (state) {
            PROCESS_WATERMARK -> {
                progTracker.notDone()
                if (pendingWatermark == null) {
                    val wm = watermarkCoalescer.checkWmHistory(now)
                    if (wm == java.lang.Long.MIN_VALUE) {
                        state = PROCESS_INBOX
                        stateMachineStep(now) // recursion
                        break
                    }
                    pendingWatermark = Watermark(wm)
                }
                if (processor.tryProcessWatermark(pendingWatermark!!)) {
                    state = EMIT_WATERMARK
                    stateMachineStep(now) // recursion
                }
            }

            EMIT_WATERMARK -> {
                progTracker.notDone()
                if (outbox.offer(pendingWatermark!!)) {
                    state = PROCESS_INBOX
                    pendingWatermark = null
                    stateMachineStep(now) // recursion
                }
            }

            PROCESS_INBOX -> {
                progTracker.notDone()
                if (inbox.isEmpty && (isSnapshotInbox || processor.tryProcess())) {
                    fillInbox(now)
                }
                if (!inbox.isEmpty) {
                    if (isSnapshotInbox) {
                        processor.restoreFromSnapshot(inbox)
                    } else {
                        processor.process(currInstream!!.ordinal(), inbox)
                    }
                }

                if (inbox.isEmpty) {
                    // there is either snapshot or instream is done, not both
                    if (currInstream != null && currInstream!!.isDone) {
                        state = COMPLETE_EDGE
                        progTracker.madeProgress()
                        return
                    } else if (context.snapshottingEnabled()
                            && numActiveOrdinals > 0
                            && receivedBarriers.cardinality() == numActiveOrdinals) {
                        // we have an empty inbox and received the current snapshot barrier from all active ordinals
                        state = SAVE_SNAPSHOT
                        return
                    } else if (numActiveOrdinals == 0) {
                        progTracker.madeProgress()
                        state = COMPLETE
                    } else {
                        state = PROCESS_WATERMARK
                    }
                }
                return
            }

            COMPLETE_EDGE -> {
                progTracker.notDone()
                if (if (isSnapshotInbox)
                            processor.finishSnapshotRestore()
                        else
                            processor.completeEdge(currInstream!!.ordinal())) {
                    progTracker.madeProgress()
                    state = initialProcessingState()
                }
                return
            }

            SAVE_SNAPSHOT -> {
                assert(context.snapshottingEnabled()) { "Snapshotting is not enabled" }

                progTracker.notDone()
                if (processor.saveToSnapshot()) {
                    progTracker.madeProgress()
                    state = EMIT_BARRIER
                }
                return
            }

            EMIT_BARRIER -> {
                assert(context.snapshottingEnabled()) { "Snapshotting is not enabled" }

                progTracker.notDone()
                if (outbox.offerToEdgesAndSnapshot(SnapshotBarrier(pendingSnapshotId))) {
                    receivedBarriers.clear()
                    pendingSnapshotId++
                    state = initialProcessingState()
                }
                return
            }

            COMPLETE -> {
                progTracker.notDone()
                // check ssContext to see if a barrier should be emitted
                if (context.snapshottingEnabled()) {
                    val currSnapshotId = ssContext!!.lastSnapshotId()
                    assert(currSnapshotId <= pendingSnapshotId) {
                        ("Unexpected new snapshot id " + currSnapshotId
                                + ", current was" + pendingSnapshotId)
                    }
                    if (currSnapshotId == pendingSnapshotId) {
                        state = SAVE_SNAPSHOT
                        progTracker.madeProgress()
                        return
                    }
                }
                if (processor.complete()) {
                    progTracker.madeProgress()
                    state = EMIT_DONE_ITEM
                }
                return
            }

            EMIT_DONE_ITEM -> {
                if (!outbox.offerToEdgesAndSnapshot(DONE_ITEM)) {
                    progTracker.notDone()
                    return
                }
                state = END
                return
            }

            else ->
                // note ProcessorState.END goes here
                throw JetException("Unexpected state: " + state!!)
        }
    }

    private fun fillInbox(now: Long) {
        if (instreamCursor == null) {
            return
        }
        val first = instreamCursor!!.value()
        var result: ProgressState
        do {
            currInstream = instreamCursor!!.value()
            result = NO_PROGRESS

            // skip ordinals where a snapshot barrier has already been received
            if (ssContext != null && ssContext.processingGuarantee() == ProcessingGuarantee.EXACTLY_ONCE
                    && receivedBarriers.get(currInstream!!.ordinal())) {
                instreamCursor!!.advance()
                continue
            }
            result = currInstream!!.drainTo(Consumer<Any> { inbox.add(it) })
            progTracker.madeProgress(result.isMadeProgress)

            if (result.isDone) {
                receivedBarriers.clear(currInstream!!.ordinal())
                watermarkCoalescer.queueDone(currInstream!!.ordinal())
                instreamCursor!!.remove()
                numActiveOrdinals--
            }

            // check if the last drained item is special
            val lastItem = inbox.peekLast()
            if (lastItem is Watermark) {
                assert(pendingWatermark == null)
                val newWmValue = (inbox.removeLast() as Watermark).timestamp()
                val wm = watermarkCoalescer.observeWm(now, currInstream!!.ordinal(), newWmValue)
                if (wm != java.lang.Long.MIN_VALUE) {
                    pendingWatermark = Watermark(wm)
                }
            } else if (lastItem is SnapshotBarrier) {
                val barrier = inbox.removeLast() as SnapshotBarrier
                observeSnapshot(currInstream!!.ordinal(), barrier.snapshotId())
            }

            // pop current priority group
            if (!instreamCursor!!.advance()) {
                instreamCursor = popInstreamGroup()
                return
            }
        } while (!result.isMadeProgress && instreamCursor!!.value() !== first)
    }

    private fun popInstreamGroup(): CircularListCursor<InboundEdgeStream>? {
        return Optional.ofNullable(instreamGroupQueue.poll())
                .map<CircularListCursor<InboundEdgeStream>>(Function<ArrayList<InboundEdgeStream>, CircularListCursor<InboundEdgeStream>> { CircularListCursor(it) })
                .orElse(null)
    }

    override fun toString(): String {
        return "ProcessorTasklet{vertex=" + context.vertexName() + ", processor=" + processor + '}'.toString()
    }

    private fun observeSnapshot(ordinal: Int, snapshotId: Long) {
        if (snapshotId != pendingSnapshotId) {
            throw JetException("Unexpected snapshot barrier " + snapshotId + " from ordinal " + ordinal +
                    " expected " + pendingSnapshotId)
        }
        receivedBarriers.set(ordinal)
    }

    /**
     * Initial state of the processor. If there are no inbound ordinals left, we will go to COMPLETE state
     * otherwise to PROCESS_INBOX.
     */
    private fun initialProcessingState(): ProcessorState {
        return if (instreamCursor == null) COMPLETE else PROCESS_INBOX
    }

    override fun isCooperative(): Boolean {
        return processor.isCooperative
    }
}
