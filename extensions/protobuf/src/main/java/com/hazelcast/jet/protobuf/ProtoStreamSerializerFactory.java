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

import com.hazelcast.jet.impl.serialization.SerializerFactory.ProtoSerializerFactory;
import com.hazelcast.nio.serialization.StreamSerializer;

import javax.annotation.Nonnull;

/**
 * An implementation of {@link ProtoSerializerFactory} constructing
 * Google Protocol Buffers {@link StreamSerializer}s.
 *
 * @since 4.1
 */
public class ProtoStreamSerializerFactory implements ProtoSerializerFactory {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Nonnull @Override
    public <T> StreamSerializer<T> create(@Nonnull Class<T> clazz, int typeId) {
        return new ProtoStreamSerializer(typeId, clazz);
    }
}
