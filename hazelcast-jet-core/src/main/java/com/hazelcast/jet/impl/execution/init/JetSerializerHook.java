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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.util.MutableLong;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

public final class JetSerializerHook {

    /**
     * Start of reserved space for Jet-specific serializers.
     * Any ID greater than this number might be used by some other Hazelcast serializer.
     * For more information, {@see SerializationConstants}
     */
    public static final int JET_RESERVED_SPACE_START = SerializationConstants.JET_SERIALIZER_FIRST;

    public static final int MAP_ENTRY = -300;
    public static final int CUSTOM_CLASS_LOADED_OBJECT = -301;
    public static final int OBJECT_ARRAY = -302;
    public static final int FRAME = -303;
    public static final int MUTABLE_LONG = -304;

    // reserved for hadoop module -380 to -390

    /**
     * End of reserved space for Jet-specific serializers.
     * Any ID less than this number might be used by some other Hazelcast serializer.
     */
    public static final int JET_RESERVED_SPACE_END = SerializationConstants.JET_SERIALIZER_LAST;

    private JetSerializerHook() {
    }

    public static final class ObjectArray implements SerializerHook<Object[]> {

        @Override
        public Class<Object[]> getSerializationType() {
            return Object[].class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Object[]>() {

                @Override
                public int getTypeId() {
                    return OBJECT_ARRAY;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, Object[] array) throws IOException {
                    out.writeInt(array.length);
                    for (int i = 0; i < array.length; i++) {
                        out.writeObject(array[i]);
                    }
                }

                @Override
                public Object[] read(ObjectDataInput in) throws IOException {
                    int length = in.readInt();
                    Object[] array = new Object[length];
                    for (int i = 0; i < array.length; i++) {
                        array[i] = in.readObject();
                    }
                    return array;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class MapEntry implements SerializerHook<Map.Entry> {

        @Override
        public Class<Entry> getSerializationType() {
            return Map.Entry.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Map.Entry>() {
                @Override
                public int getTypeId() {
                    return MAP_ENTRY;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, Entry object) throws IOException {
                    out.writeObject(object.getKey());
                    out.writeObject(object.getValue());
                }

                @Override
                public Entry read(ObjectDataInput in) throws IOException {
                    return entry(in.readObject(), in.readObject());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class FrameSerializer implements SerializerHook<Frame> {

        @Override
        public Class<Frame> getSerializationType() {
            return Frame.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Frame>() {
                @Override
                public void write(ObjectDataOutput out, Frame object) throws IOException {
                    out.writeLong(object.getSeq());
                    out.writeObject(object.getKey());
                    out.writeObject(object.getValue());
                }

                @Override
                public Frame read(ObjectDataInput in) throws IOException {
                    long seq = in.readLong();
                    Object key = in.readObject();
                    Object value = in.readObject();
                    return new Frame<>(seq, key, value);
                }

                @Override
                public int getTypeId() {
                    return JetSerializerHook.FRAME;
                }

                @Override
                public void destroy() {
                }
            };
        }

        @Override public boolean isOverwritable() {
            return false;
        }
    }
    public static final class MutableLongSerializer implements SerializerHook<MutableLong> {

        @Override
        public Class<MutableLong> getSerializationType() {
            return MutableLong.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<MutableLong>() {
                @Override
                public int getTypeId() {
                    return MUTABLE_LONG;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, MutableLong object) throws IOException {
                    out.writeLong(object.value);
                }

                @Override
                public MutableLong read(ObjectDataInput in) throws IOException {
                    return MutableLong.valueOf(in.readLong());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }
}
