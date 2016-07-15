/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.data.tuple;


import com.hazelcast.jet.data.tuple.JetTuple;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * Represents abstract converter from Java-representation onto the tuple representation;
 *
 * For example Map's entry {@literal <Key,Value>} will be represented as
 *
 * Tuple with [Key] as key part and [Value] as value part;
 *
 * @param <R> - type of the input Java-object;
 */
@FunctionalInterface
public interface JetTupleConverter<R> {
    /**
     * Performs converting of java-heap object into the tuple representation of object;
     *
     * @param object - java object;
     * @param ss     - Hazelcast serialization  service;
     * @return - corresponding tuple representation;
     */
    JetTuple convert(R object, SerializationService ss);
}
