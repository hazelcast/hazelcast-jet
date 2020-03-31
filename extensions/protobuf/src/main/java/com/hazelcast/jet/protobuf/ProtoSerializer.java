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
import com.google.protobuf.Parser;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * An adapter implementation of {@link StreamSerializer} for Google Protocol
 * Buffers v3 binary format.
 *
 * <p>To learn more about Protocol Buffers, visit:
 * <a href="https://developers.google.com/protocol-buffers/docs/proto3">
 * https://developers.google.com/protocol-buffers/docs/proto3
 * </a>
 *
 * @param <T> the Protocol Buffers {@link GeneratedMessageV3} type handled by
 *            this {@link StreamSerializer}.
 * @since 4.1
 */
public abstract class ProtoSerializer<T extends GeneratedMessageV3> implements StreamSerializer<T> {

    private static final String DEFAULT_INSTANCE_METHOD_NAME = "getDefaultInstance";

    private final int typeId;
    private final Parser<T> parser;

    /**
     * Creates Protocol Buffers v3 serializer.
     *
     * @param clazz  {@link GeneratedMessageV3} type handled by this
     *               serializer
     * @param typeId unique type id of this serializer
     */
    public ProtoSerializer(@Nonnull Class<T> clazz, int typeId) {
        checkTrue(GeneratedMessageV3.class.isAssignableFrom(clazz), clazz.getName() + " is not supported, " +
                "provide a Protocol Buffers " + GeneratedMessageV3.class.getName() + " type");

        this.typeId = typeId;
        this.parser = parser(clazz);
    }

    @SuppressWarnings("unchecked")
    private Parser<T> parser(Class<T> clazz) {
        try {
            T defaultMessageInstance = (T) clazz.getMethod(DEFAULT_INSTANCE_METHOD_NAME).invoke(null);
            return (Parser<T>) defaultMessageInstance.getParserForType();
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public int getTypeId() {
        return typeId;
    }

    @Override
    public void write(ObjectDataOutput out, T object) throws IOException {
        out.writeByteArray(object.toByteArray());
    }

    @Override
    public T read(ObjectDataInput in) throws IOException {
        return parser.parseFrom(in.readByteArray());
    }

    /**
     * An utility method that creates an anonymous {@link ProtoSerializer}.
     *
     * @param clazz  {@link GeneratedMessageV3} type of created serializer
     * @param typeId unique type id of created serializer
     * @param <T>    the Protocol Buffers {@link GeneratedMessageV3} type
     *               handled by created {@link StreamSerializer}
     */
    @Nonnull
    public static <T extends GeneratedMessageV3> ProtoSerializer<T> from(@Nonnull Class<T> clazz, int typeId) {
        return new ProtoSerializer<T>(clazz, typeId) {
        };
    }
}
