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

package com.hazelcast.jet.function;

import java.io.Serializable;
import java.util.function.LongToDoubleFunction;

/**
 * Represents a function that accepts a long-valued argument and produces a
 * double-valued result.  This is the {@code long}-to-{@code double} primitive
 * specialization for {@link DistributedFunction}.
 *
 * <p>This is a functional interface
 * whose functional method is {@link #applyAsDouble(long)}.
 *
 * @see DistributedFunction
 */
@FunctionalInterface
public interface DistributedLongToDoubleFunction extends LongToDoubleFunction, Serializable {
}
