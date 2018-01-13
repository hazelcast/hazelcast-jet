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

import com.hazelcast.jet.aggregate.AggregateOperation1
import com.hazelcast.jet.core.Inbox
import java.util.function.Function as JavaFunction

fun <T, A, R> aggregatePK(aggrOp: AggregateOperation1<T, A, R>) =
        KotlinWrapperP(AggregatePK(aggrOp))

class AggregatePK< T, A, R>(aggrOp: AggregateOperation1<T, A, R>) : AbstractProcessorK() {
    override var isCooperative = true

    private val accumulateFn = aggrOp.accumulateFn()
    private val finishFn = aggrOp.finishFn()
    private val acc = aggrOp.createFn().get()

    override suspend fun process(ordinal: Int, inbox: Inbox) = inbox.drain {
        @Suppress("UNCHECKED_CAST")
        accumulateFn.accept(acc, it as T)
    }

    override suspend fun complete() {
        emit(finishFn.apply(acc)!!)
    }
}
