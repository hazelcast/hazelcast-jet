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

@file:Suppress("NOTHING_TO_INLINE")

package com.hazelcast.jet.core.kotlin

import com.hazelcast.jet.core.BroadcastKey
import com.hazelcast.jet.core.BroadcastKey.broadcastKey
import com.hazelcast.jet.core.Inbox
import com.hazelcast.jet.core.WatermarkGenerationParams
import com.hazelcast.jet.core.WatermarkSourceUtil
import com.hazelcast.jet.impl.util.LoggingUtil.logFine

fun <T> insertWatermarksPK(
        wmGenParams: WatermarkGenerationParams<T>
) = KotlinWrapperP(InsertWatermarksPK(wmGenParams))

class InsertWatermarksPK<T>(
        wmGenParams: WatermarkGenerationParams<T>
): AbstractProcessorK() {
    override var isCooperative = true

    private val wsu = WatermarkSourceUtil<T>(wmGenParams)
    init {
        wsu.increasePartitionCount(1)
    }

    private var lastEmittedWm = Long.MIN_VALUE

    // value to be used temporarily during snapshot restore
    private var minRestoredWm = Long.MAX_VALUE

    override suspend fun process() {
        val wm = wsu.handleNoEvent()
        wm?.also { emit(it) }
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun process(ordinal: Int, inbox: Inbox) = inbox.drain { item ->
        val wm = wsu.handleEvent(0, item as T)
        wm?.also { emit(it) }
        emit(item)
    }

    override suspend fun saveToSnapshot() {
        emitToSnapshot(broadcastKey(Keys.LAST_EMITTED_WM), lastEmittedWm)
    }

    override suspend fun restoreFromSnapshot(inbox: Inbox) = inbox.drainSnapshot { key, value ->
        assert((key as? BroadcastKey<*>)?.key() == Keys.LAST_EMITTED_WM) { "Unexpected key: " + key }
        // we restart at the oldest WM any instance was at at the time of snapshot
        minRestoredWm = Math.min(minRestoredWm, value as Long)
    }

    override suspend fun finishSnapshotRestore() {
        lastEmittedWm = minRestoredWm
        logFine(logger, "restored lastEmittedWm=%s", lastEmittedWm)
    }

    private enum class Keys {
        LAST_EMITTED_WM
    }
}
