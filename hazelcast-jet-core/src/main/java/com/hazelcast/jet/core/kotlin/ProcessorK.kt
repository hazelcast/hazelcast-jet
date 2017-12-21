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
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.suspendCoroutine

/**
 * Javadoc pending.
 */
interface ProcessorK {
    var isCooperative: Boolean
    var suspendAction: (Continuation<Any>) -> Unit

    suspend fun yield() = suspendCoroutine(suspendAction)

    fun init(outbox: Outbox, context: Processor.Context) = Unit
    suspend fun process(ordinal: Int, inbox: Inbox) = Unit
    suspend fun process() = Unit
    suspend fun completeEdge() = Unit
    suspend fun complete() = Unit
    suspend fun saveToSnapshot() = Unit
    suspend fun restoreFromSnapshot(inbox: Inbox) = Unit
    suspend fun finishSnapshotRestore() = Unit
}
