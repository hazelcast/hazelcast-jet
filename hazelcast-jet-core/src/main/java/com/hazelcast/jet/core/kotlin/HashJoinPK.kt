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
import com.hazelcast.jet.datamodel.ItemsByTag
import com.hazelcast.jet.datamodel.Tag
import com.hazelcast.jet.datamodel.Tuple2.tuple2
import com.hazelcast.jet.datamodel.Tuple3.tuple3
import java.util.function.Function as JavaFunction

fun <T0> hashJoinPK(
        keyFns: List<JavaFunction<in T0, Any>>,
        tags: List<Tag<Any?>>
) = KotlinWrapperP(HashJoinPK(keyFns, tags))

class HashJoinPK<T0>(
        keyFns: List<JavaFunction<in T0, Any>>,
        private val tags: List<Tag<Any?>>
): AbstractProcessorK() {
    override var isCooperative = true

    private val keyFns = keyFns.slice(0..0) + keyFns // item 0 of this list won't be used
    private val lookupTables: Array<Map<Any, Any>?> = arrayOfNulls(keyFns.size + 1)
    private var ordinal0consumed = false

    suspend override fun process(ordinal: Int, inbox: Inbox) = when (ordinal) {
        0 -> inbox.drain {
            @Suppress("UNCHECKED_CAST")
            val t0 = it as T0
            ordinal0consumed = true
            if (tags.isEmpty()) {
                emit(if (keyFns.size == 2)
                    tuple2<T0, Any>(t0, lookupJoined(1, t0)) else
                    tuple3<T0, Any, Any>(t0, lookupJoined(1, t0), lookupJoined(2, t0)))
            }
            val ibt = ItemsByTag()
            for (i in 1 until keyFns.size) {
                ibt.put(tags[i], lookupJoined(i, t0))
            }
            emit(tuple2<T0, ItemsByTag>(t0, ibt))
        }
        else -> inbox.drain {
            assert(!ordinal0consumed) { "Edge 0 must have a lower priority than all other edges" }
            @Suppress("UNCHECKED_CAST")
            lookupTables[ordinal] = it as Map<Any, Any>
        }
    }

    private fun lookupJoined(ordinal: Int, item: T0): Any? = lookupTables[ordinal]!![keyFns[ordinal].apply(item)]
}
