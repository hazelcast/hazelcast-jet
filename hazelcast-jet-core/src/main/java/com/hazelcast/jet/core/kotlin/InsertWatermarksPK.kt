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

import com.hazelcast.jet.core.*
import com.hazelcast.jet.core.BroadcastKey.broadcastKey
import com.hazelcast.jet.impl.util.LoggingUtil.logFine
import java.util.function.ToLongFunction

fun <T> insertWatermarksPK(
        getTimestampFn: ToLongFunction<T>,
        wmPolicy: WatermarkPolicy,
        wmEmitPolicy: WatermarkEmissionPolicy
) = KotlinWrapperP(InsertWatermarksPK(getTimestampFn, wmPolicy, wmEmitPolicy))

class InsertWatermarksPK<T>(
        private val getTimestampFn: ToLongFunction<T>,
        private val wmPolicy: WatermarkPolicy,
        private val wmEmitPolicy: WatermarkEmissionPolicy
): AbstractProcessorK() {
    override var isCooperative = true

    private var currWm = Long.MIN_VALUE
    private var lastEmittedWm = Long.MIN_VALUE

    // value to be used temporarily during snapshot restore
    private var minRestoredWm = Long.MAX_VALUE

    override suspend fun process() {
        currWm = wmPolicy.currentWatermark
        emitWmIfIndicated()
    }

    override suspend fun process(ordinal: Int, inbox: Inbox) {
        inbox.drain {
            @Suppress("UNCHECKED_CAST")
            val timestamp = getTimestampFn.applyAsLong(it as T)
            currWm = wmPolicy.reportEvent(timestamp)
            emitWmIfIndicated()
            if (timestamp >= currWm) {
                emit(it)
            } else {
                logger.takeIf { it.isInfoEnabled }?.apply { info("Dropped late event: $it") }
            }
        }
    }

    override suspend fun saveToSnapshot() {
        emitToSnapshot(broadcastKey(Keys.LAST_EMITTED_WM), lastEmittedWm)
    }

    override suspend fun restoreFromSnapshot(key: Any, value: Any) {
        assert((key as BroadcastKey<*>).key() == Keys.LAST_EMITTED_WM) { "Unexpected key: " + key }
        // we restart at the oldest WM any instance was at at the time of snapshot
        minRestoredWm = Math.min(minRestoredWm, value as Long)
    }

    override suspend fun finishSnapshotRestore() {
        lastEmittedWm = minRestoredWm
        logFine(logger, "restored lastEmittedWm=%s", lastEmittedWm)
    }

    private inline suspend fun emitWmIfIndicated() {
        if (wmEmitPolicy.shouldEmit(currWm, lastEmittedWm)) {
            emit(Watermark(currWm))
            lastEmittedWm = currWm
        }
    }

    private enum class Keys {
        LAST_EMITTED_WM
    }
}
