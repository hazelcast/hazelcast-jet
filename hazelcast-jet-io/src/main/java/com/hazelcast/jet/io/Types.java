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

package com.hazelcast.jet.io;

import com.hazelcast.jet.io.impl.readers.BooleanReader;
import com.hazelcast.jet.io.impl.readers.CharReader;
import com.hazelcast.jet.io.impl.readers.DoubleReader;
import com.hazelcast.jet.io.impl.readers.FloatReader;
import com.hazelcast.jet.io.impl.readers.ShortReader;
import com.hazelcast.jet.io.impl.readers.ByteReader;
import com.hazelcast.jet.io.impl.readers.IntReader;
import com.hazelcast.jet.io.impl.readers.LongReader;
import com.hazelcast.jet.io.impl.readers.Tuple2Reader;
import com.hazelcast.jet.io.impl.readers.StringReader;
import com.hazelcast.jet.io.impl.writers.StringWriter;
import com.hazelcast.jet.io.impl.writers.BooleanWriter;
import com.hazelcast.jet.io.impl.writers.CharWriter;
import com.hazelcast.jet.io.impl.writers.FloatWriter;
import com.hazelcast.jet.io.impl.writers.DoubleWriter;
import com.hazelcast.jet.io.impl.writers.ShortWriter;
import com.hazelcast.jet.io.impl.writers.ByteWriter;
import com.hazelcast.jet.io.impl.writers.IntWriter;
import com.hazelcast.jet.io.impl.writers.LongWriter;
import com.hazelcast.jet.io.impl.writers.Tuple2Writer;
import com.hazelcast.jet.io.impl.readers.NullObjectReader;
import com.hazelcast.jet.io.impl.writers.NullObjectWriter;
import com.hazelcast.jet.io.tuple.DefaultTupleFactory;
import com.hazelcast.jet.io.impl.readers.DefaultObjectReader;
import com.hazelcast.jet.io.impl.writers.DefaultObjectWriter;

import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.util.collection.Int2ObjectHashMap;

import java.util.IdentityHashMap;
import java.util.Map;

//CHECKSTYLE:OFF
public enum Types implements DataType {
    BOOLEAN((byte) -12, Boolean.class, new BooleanWriter(), new BooleanReader()),
    CHAR((byte) -11, Character.class, new CharWriter(), new CharReader()),
    FLOAT((byte) -10, Float.class, new FloatWriter(), new FloatReader()),
    DOUBLE((byte) -9, Double.class, new DoubleWriter(), new DoubleReader()),
    SHORT((byte) -8, Short.class, new ShortWriter(), new ShortReader()),
    BYTE((byte) -7, Byte.class, new ByteWriter(), new ByteReader()),
    INT((byte) -6, Integer.class, new IntWriter(), new IntReader()),
    LONG((byte) -5, Long.class, new LongWriter(), new LongReader()),
    // -4 Reserved for JetTuple
    TUPLE2((byte) -3, Tuple2.class, new Tuple2Writer(), new Tuple2Reader(new DefaultTupleFactory())),
    STRING((byte) -2,
            String.class,
            new StringWriter(),
            new StringReader()
    ),
    OBJECT((byte) -1,
            Object.class,
            new DefaultObjectWriter(),
            new DefaultObjectReader()
    ),
    NULL(NULL_TYPE_ID,
            null,
            new NullObjectWriter(),
            new NullObjectReader()
    );

    private static final Class[] CLASSES;

    private static final Map<Class, DataType> CLASSES2_TYPES = new IdentityHashMap<Class, DataType>();

    private static final Map<Integer, DataType> TYPES = new Int2ObjectHashMap<DataType>();

    static {
        CLASSES = new Class[Types.values().length - 2];
        int i = 0;

        for (Types type : Types.values()) {
            if (type.getClazz() == null || (type.getClazz().equals(Object.class))) {
                continue;
            }

            TYPES.put((int) type.getTypeID(), type);
            CLASSES2_TYPES.put(type.getClazz(), type);
            CLASSES[i] = type.getClazz();
            i++;
        }
    }

    private final byte typeID;
    private final Class clazz;
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    Types(byte typeID,
          Class clazz,
          ObjectWriter objectWriter,
          ObjectReader objectReader) {
        this.clazz = clazz;
        this.typeID = typeID;
        this.objectWriter = objectWriter;
        this.objectReader = objectReader;
    }

    public static DataType getDataType(byte typeID) {
        DataType dataType = TYPES.get((int) typeID);

        if (dataType == null) {
            dataType = OBJECT;
        }

        return dataType;
    }

    public static DataType getDataType(Object object) {
        if (object == null) {
            return Types.NULL;
        }

        for (Class<?> clazz : CLASSES) {
            if (clazz.isAssignableFrom(object.getClass())) {
                return CLASSES2_TYPES.get(clazz);
            }
        }

        return Types.OBJECT;
    }

    @Override
    public Class getClazz() {
        return this.clazz;
    }

    @Override
    public byte getTypeID() {
        return this.typeID;
    }

    @Override
    public ObjectWriter getObjectWriter() {
        return this.objectWriter;
    }

    @Override
    public ObjectReader getObjectReader() {
        return this.objectReader;
    }
}
//CHECKSTYLE:ON
