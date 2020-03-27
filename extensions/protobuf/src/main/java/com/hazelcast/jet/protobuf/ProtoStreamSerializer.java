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

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * An adapter implementation of {@link StreamSerializer} for Google Protocol
 * Buffers binary format.
 *
 * <p>To learn more about Protocol Buffers, visit:
 * <a href="https://developers.google.com/protocol-buffers">https://developers.google.com/protocol-buffers</a>
 *
 * @param <T> the Protocol Buffers {@link Message} handled by this
 *            {@link StreamSerializer}.
 * @since 4.1
 */
public class ProtoStreamSerializer<T extends Message> implements StreamSerializer<T> {

    private static final String DEFAULT_INSTANCE_METHOD_NAME = "getDefaultInstance";

    private final int typeId;
    private final Parser<T> parser;

    /**
     * Creates Google Protocol Buffers serializer.
     *
     * @param typeId unique type id of serializer
     * @param clazz  class of {@link Message} handled by this serializer
     */
    public ProtoStreamSerializer(int typeId, @Nonnull Class<T> clazz) {
        checkTrue(Message.class.isAssignableFrom(clazz), clazz.getName() + " is not supported");
        checkFalse(DynamicMessage.class.isAssignableFrom(clazz), "DynamicMessage is not supported");

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
}
