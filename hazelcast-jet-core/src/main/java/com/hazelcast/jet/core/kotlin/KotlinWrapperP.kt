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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.launch
import kotlin.coroutines.experimental.Continuation
open class KotlinWrapperP(
        private val wrapped: ProcessorK
) : Processor {
    private var continuation: Continuation<Unit>? = null
    init {
        wrapped.suspendAction = { this.continuation = it }
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private fun resumeOrLaunch(block : suspend () -> Unit) {
        val currentContinuation = continuation
        if (currentContinuation != null) {
            continuation = null
            currentContinuation.resume(Unit)
        } else {
            launch(Unconfined) {
                block()
                continuation = null
            }
        }
    }

    override fun isCooperative() = wrapped.isCooperative

    fun setCooperative(value: Boolean) {
        wrapped.isCooperative = value
    }

    override fun init(outbox: Outbox, context: Processor.Context) {
        wrapped.init(outbox, context)
    }

    override fun process(ordinal: Int, inbox: Inbox) {
        resumeOrLaunch { wrapped.process(ordinal, inbox) }
    }

    override fun tryProcess(): Boolean {
        resumeOrLaunch { wrapped.process() }
        return continuation == null
    }

    override fun completeEdge(ordinal: Int): Boolean {
        resumeOrLaunch { wrapped.completeEdge() }
        return continuation == null
    }

    override fun complete(): Boolean {
        resumeOrLaunch { wrapped.complete() }
        return continuation == null
    }

    override fun saveToSnapshot(): Boolean {
        resumeOrLaunch { wrapped.saveToSnapshot() }
        return continuation == null
    }

    override fun restoreFromSnapshot(inbox: Inbox) {
        resumeOrLaunch { wrapped.restoreFromSnapshot(inbox) }
    }

    override fun finishSnapshotRestore(): Boolean {
        resumeOrLaunch { wrapped.finishSnapshotRestore() }
        return continuation == null
    }

}

