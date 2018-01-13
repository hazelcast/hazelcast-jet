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
import java.util.*
import java.util.function.Function as JavaFunction

fun <T, K, V> hashJoinCollectPK(
        keyFn: JavaFunction<in T, out K>,
        projectFn: JavaFunction<in T, out V>
) = KotlinWrapperP(HashJoinCollectPK(keyFn, projectFn))

class HashJoinCollectPK<T, K, V>(
        private val keyFn: JavaFunction<in T, out K>,
        private val projectFn: JavaFunction<in T, out V>
): AbstractProcessorK() {
    override var isCooperative = true

    private val map = HashMap<K, V>()

    override suspend fun process(ordinal: Int, inbox: Inbox) = inbox.drain {
        @Suppress("UNCHECKED_CAST")
        val t = it as T
        val key = keyFn.apply(t)
        val value = projectFn.apply(t)
        val previous = map.put(key, value)
        if (previous != null) {
            throw IllegalStateException("Duplicate values for key $key: $previous and $value")
        }
    }

    override suspend fun complete() {
        for (e in map) {
            emit(e)
        }
    }
}
