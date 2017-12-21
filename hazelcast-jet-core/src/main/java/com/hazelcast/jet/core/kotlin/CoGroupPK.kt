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

import com.hazelcast.jet.Util.entry
import com.hazelcast.jet.aggregate.AggregateOperation
import com.hazelcast.jet.aggregate.AggregateOperation1
import java.util.Collections.singletonList
import java.util.function.Function as JavaFunction

fun <T, K, A, R> coGroupPK(keyFn: JavaFunction<in T, out K>, aggrOp: AggregateOperation1<in T, A, R>)
    = KotlinWrapperP(CoGroupPK(singletonList(keyFn), aggrOp))

fun <K, A, R> coGroupPK(keyFns: List<JavaFunction<*, out K>>, aggrOp: AggregateOperation<A, R>)
    = KotlinWrapperP(CoGroupPK(keyFns, aggrOp))

class CoGroupPK<K, A, R>(
        private val keyFns: List<JavaFunction<*, out K>>,
        private val aggrOp: AggregateOperation<A, R>
) : AbstractProcessorK() {
    override var isCooperative = true

    private val keyToAcc = HashMap<K, A>()

    suspend override fun process(ordinal: Int, item: Any) {
        @Suppress("UNCHECKED_CAST")
        val keyFn = keyFns[ordinal] as JavaFunction<Any, K>
        val acc = keyToAcc.computeIfAbsent(keyFn.apply(item), { aggrOp.createFn().get() })
        aggrOp.accumulateFn<Any>(ordinal).accept(acc, item)
    }

    override suspend fun complete() {
        for ((k, a) in keyToAcc) {
            emit(entry(k, aggrOp.finishFn().apply(a)))
        }
    }
}
