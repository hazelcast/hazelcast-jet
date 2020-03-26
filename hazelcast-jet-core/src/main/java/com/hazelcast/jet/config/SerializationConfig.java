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
import com.hazelcast.nio.serialization.StreamSerializer;

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
 */
public class SerializationConfig implements Serializable {

    static final long serialVersionUID = 1L;

    private final Map<String, SerializerPrimer> primersByClass;

    public SerializationConfig() {
        this.primersByClass = new HashMap<>();
    }

    public <T, S extends StreamSerializer<?>> void registerSerializer(@Nonnull Class<T> clazz,
                                                                      @Nonnull Class<S> serializerClass) {
        register(clazz, new StreamSerializerPrimer(serializerClass.getName()));
    }

    public <T> void registerProtobufSerializer(@Nonnull Class<T> clazz,
                                               int typeId) {
        register(clazz, new ProtobufSerializerPrimer(clazz.getName(), typeId));
    }

    private void register(Class<?> clazz, SerializerPrimer primer) {
        checkFalse(primersByClass.containsKey(clazz.getName()), "Serializer for " + clazz + " already registered");
        checkFalse(primersByClass.containsValue(primer), "Serializer " + primer + " already registered");
        primersByClass.put(clazz.getName(), primer);
    }

    public boolean isEmpty() {
        return primersByClass.isEmpty();
    }

    @Nonnull
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

        StreamSerializer<?> construct(@Nonnull SerializerFactory serializerFactory);
    }

    static class StreamSerializerPrimer implements SerializerPrimer {

        static final long serialVersionUID = 1L;

        private final String serializerClassName;

        StreamSerializerPrimer(@Nonnull String serializerClassName) {
            this.serializerClassName = serializerClassName;
        }

        @Override
        public StreamSerializer<?> construct(@Nonnull SerializerFactory serializerFactory) {
            return serializerFactory.createSerializer(serializerClassName);
        }

        @Override
        public String toString() {
            return "Serializer{" +
                    "serializerClassName='" + serializerClassName + '\'' +
                    '}';
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

    static class ProtobufSerializerPrimer implements SerializerPrimer {

        static final long serialVersionUID = 1L;

        private final String className;
        private final int typeId;

        ProtobufSerializerPrimer(@Nonnull String className, int typeId) {
            this.className = className;
            this.typeId = typeId;
        }

        @Override
        public StreamSerializer<?> construct(@Nonnull SerializerFactory serializerFactory) {
            return serializerFactory.createProtobufSerializer(className, typeId);
        }

        @Override
        public String toString() {
            return "ProtobufSerializer{" +
                    "className='" + className + '\'' +
                    ", typeId=" + typeId +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProtobufSerializerPrimer that = (ProtobufSerializerPrimer) o;
            return typeId == that.typeId &&
                    className.equals(that.className);
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, typeId);
        }
    }
}
