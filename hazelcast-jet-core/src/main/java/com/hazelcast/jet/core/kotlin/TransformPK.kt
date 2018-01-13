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

import com.hazelcast.jet.Traverser
import com.hazelcast.jet.core.Inbox
import java.util.function.Function as JavaFunction

fun <T, R> transformPK(
        mapFn: JavaFunction<T, out Traverser<out R>>
) = KotlinWrapperP(TransformPK(mapFn))

class TransformPK<T, out R>(
        private val mapFn: JavaFunction<T, out Traverser<out R>>
) : AbstractProcessorK() {
    override var isCooperative = true

    override suspend fun process(ordinal: Int, inbox: Inbox) = inbox.drain {
        @Suppress("UNCHECKED_CAST")
        val outTrav = mapFn.apply(it as T)
        while (true) {
            emit(outTrav.next() ?: break)
        }
    }
}
