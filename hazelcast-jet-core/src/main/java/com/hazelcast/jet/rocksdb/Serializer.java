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

package com.hazelcast.jet.rocksdb;

import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.internal.serialization.impl.SerializerAdapter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.serialization.DelegatingSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static java.util.Collections.emptyMap;

public class Serializer<T> {

    private final DelegatingSerializationService serializationService;
    private SerializerAdapter serializer;

    public Serializer() {
        AbstractSerializationService delegate = SerializationServiceV1.builder().build();
        serializationService = new DelegatingSerializationService(emptyMap(), delegate);
    }

    public byte[] serialize(T item) throws JetException {
        if (serializer == null) {
            serializer = serializationService.serializerFor(item);
        }
        ObjectDataOutput out = serializationService.createObjectDataOutput();
        try {
            serializer.write(out, item);
            return out.toByteArray();
        } catch (IOException e) {
            throw new JetException(e.getMessage(), e.getCause());
        }
    }

    @SuppressWarnings("unchecked")
    public T deserialize(byte[] item) throws JetException {
        try {
            ObjectDataInput in = serializationService.createObjectDataInput(item);
            return (T) serializer.read(in);
        } catch (IOException e) {
            throw new JetException(e.getMessage(), e.getCause());
        }
    }
}
