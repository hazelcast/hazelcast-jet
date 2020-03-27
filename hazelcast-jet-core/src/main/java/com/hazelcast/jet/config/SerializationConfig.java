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

package com.hazelcast.jet.config;

import com.hazelcast.jet.impl.serialization.SerializerFactory;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.Preconditions.checkFalse;

/**
 * Contains the serialization configuration specific to one Hazelcast Jet job.
 *
 * @since 4.1
 */
@PrivateApi
public class SerializationConfig implements Serializable {

    static final long serialVersionUID = 1L;

    private final Map<String, SerializerPrimer> primersByClass;

    public SerializationConfig() {
        this.primersByClass = new HashMap<>();
    }

    /**
     * Registers given {@code serializerClass} for given {@code clazz}.
     */
    public <T, S extends StreamSerializer<?>> void registerSerializer(@Nonnull Class<T> clazz,
                                                                      @Nonnull Class<S> serializerClass) {
        register(clazz, new StreamSerializerPrimer(serializerClass.getName()));
    }

    /**
     * Registers a Google Protocol Buffers serializer with given {@code typeId} for
     * given {@code clazz}.
     */
    public <T> void registerProtoSerializer(@Nonnull Class<T> clazz,
                                            int typeId) {
        register(clazz, new ProtoSerializerPrimer(clazz.getName(), typeId));
    }

    private void register(Class<?> clazz, SerializerPrimer primer) {
        checkFalse(primersByClass.containsKey(clazz.getName()), "Serializer for " + clazz + " already registered");
        primersByClass.put(clazz.getName(), primer);
    }

    /**
     * Returns {@code true} if no serializers were registered.
     */
    public boolean isEmpty() {
        return primersByClass.isEmpty();
    }

    /**
     * Returns all the registered serializer configs.
     */
    public Stream<Entry<String, SerializerPrimer>> primers() {
        return primersByClass.entrySet().stream();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SerializationConfig that = (SerializationConfig) o;
        return Objects.equals(primersByClass, that.primersByClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primersByClass);
    }

    public interface SerializerPrimer extends Serializable {

        Serializer construct(@Nonnull SerializerFactory serializerFactory);
    }

    static class StreamSerializerPrimer implements SerializerPrimer {

        static final long serialVersionUID = 1L;

        private final String serializerClassName;

        StreamSerializerPrimer(@Nonnull String serializerClassName) {
            this.serializerClassName = serializerClassName;
        }

        @Override
        public Serializer construct(@Nonnull SerializerFactory serializerFactory) {
            return serializerFactory.createSerializer(serializerClassName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StreamSerializerPrimer that = (StreamSerializerPrimer) o;
            return serializerClassName.equals(that.serializerClassName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(serializerClassName);
        }
    }

    static class ProtoSerializerPrimer implements SerializerPrimer {

        static final long serialVersionUID = 1L;

        private final String className;
        private final int typeId;

        ProtoSerializerPrimer(@Nonnull String className, int typeId) {
            this.className = className;
            this.typeId = typeId;
        }

        @Override
        public Serializer construct(@Nonnull SerializerFactory serializerFactory) {
            return serializerFactory.createProtoSerializer(className, typeId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProtoSerializerPrimer that = (ProtoSerializerPrimer) o;
            return typeId == that.typeId &&
                    className.equals(that.className);
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, typeId);
        }
    }
}
