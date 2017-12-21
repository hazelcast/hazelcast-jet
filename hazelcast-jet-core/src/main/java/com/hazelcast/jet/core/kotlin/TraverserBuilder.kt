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
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.EmptyCoroutineContext
import kotlin.coroutines.experimental.RestrictsSuspension
import kotlin.coroutines.experimental.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.experimental.intrinsics.createCoroutineUnchecked
import kotlin.coroutines.experimental.intrinsics.suspendCoroutineOrReturn

fun <T> buildTraverser(builderAction: suspend TraverserBuilder<T>.() -> Unit): Traverser<T> {
    val trav = TraverserBuilderInternal<T>()
    trav.nextStep = builderAction.createCoroutineUnchecked(receiver = trav, completion = trav)
    return trav
}

@RestrictsSuspension
abstract class TraverserBuilder<in T> internal constructor() {
    abstract suspend fun yield(value: T)
}

private class TraverserBuilderInternal<T> : TraverserBuilder<T>(), Traverser<T>, Continuation<Unit> {
    private var nextValue: T? = null
    var nextStep: Continuation<Unit>? = null

    override suspend fun yield(value: T) {
        nextValue = value
        return suspendCoroutineOrReturn { c ->
            nextStep = c
            COROUTINE_SUSPENDED
        }
    }

    override fun next(): T? {
        val nextStep = nextStep ?: return null
        nextStep.resume(Unit)
        return nextValue
    }

    override val context: CoroutineContext
        get() = EmptyCoroutineContext

    override fun resume(value: Unit) {
        nextValue = null
        nextStep = null
    }

    override fun resumeWithException(exception: Throwable) {
        throw exception
    }
}

fun main(args: Array<String>) {
    val trav = buildTraverser {
        yield(1)
        yield(null)
        yield(2)
        throw Exception("did do done")
    }
    repeat(5) {
        val next: Int? = trav.next()
        println(next)
    }
}
