/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.serialization;

import com.hazelcast.jet.JetException;

class SerializationFailure extends JetException {

    SerializationFailure(Class<?> clazz) {
        super("There is no suitable serializer for " + clazz +
                ", did you register it with JobConfig.registerSerializer()?");
    }

    SerializationFailure(Class<?> clazz, Throwable t) {
        super("Unable to serialize instance of " + clazz +
                ". Note: You can register a serializer using JobConfig.registerSerializer()", t);
    }

    SerializationFailure(int typeId) {
        super("There is no suitable de-serializer for type " + typeId + ". "
                + "This exception is likely caused by differences in the serialization configuration between members "
                + "or between clients and members.");
    }

    SerializationFailure(int typeId, Throwable t) {
        super("Unable to deserialize object for type " + typeId, t);
    }
}
