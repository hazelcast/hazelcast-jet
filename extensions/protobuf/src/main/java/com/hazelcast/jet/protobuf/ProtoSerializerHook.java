/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.protobuf;

import com.google.protobuf.GeneratedMessageV3;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;

/**
 * This class simplifies automatic, hook based, Protocol Buffers serializers
 * registration from user modules.
 * <p>
 * Class extending this one needs to be registered using a file called
 * "com.hazelcast.SerializerHook" in META-INF/services.
 *
 * @param <T> the Protocol Buffers {@link GeneratedMessageV3} type handled by
 *            this {@link SerializerHook}.
 */
public abstract class ProtoSerializerHook<T extends GeneratedMessageV3> implements SerializerHook<T> {

    private final Class<T> clazz;
    private final int typeId;

    /**
     * Creates Protocol Buffers v3 serializer hook.
     *
     * @param clazz  {@link GeneratedMessageV3} serialization type registered
     *               by this hook
     * @param typeId unique type id of serializer registered by this hook
     */
    protected ProtoSerializerHook(Class<T> clazz, int typeId) {
        this.clazz = clazz;
        this.typeId = typeId;
    }

    @Override
    public Class<T> getSerializationType() {
        return clazz;
    }

    @Override
    public Serializer createSerializer() {
        return ProtoSerializer.from(clazz, typeId);
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
