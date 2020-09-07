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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.sql.impl.QueryException;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PortableUpsertTargetTest {

    @Test
    public void test_set() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addUTFField("string")
                        .addCharField("character")
                        .addBooleanField("boolean")
                        .addByteField("byte")
                        .addShortField("short")
                        .addIntField("int")
                        .addLongField("long")
                        .addFloatField("float")
                        .addDoubleField("double")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector stringFieldInjector = target.createInjector("string");
        UpsertInjector characterFieldInjector = target.createInjector("character");
        UpsertInjector booleanFieldInjector = target.createInjector("boolean");
        UpsertInjector byteFieldInjector = target.createInjector("byte");
        UpsertInjector shortFieldInjector = target.createInjector("short");
        UpsertInjector intFieldInjector = target.createInjector("int");
        UpsertInjector longFieldInjector = target.createInjector("long");
        UpsertInjector floatFieldInjector = target.createInjector("float");
        UpsertInjector doubleFieldInjector = target.createInjector("double");

        target.init();
        stringFieldInjector.set("1");
        characterFieldInjector.set('1');
        booleanFieldInjector.set(true);
        byteFieldInjector.set((byte) 1);
        shortFieldInjector.set((short) 1);
        intFieldInjector.set(1);
        longFieldInjector.set(1L);
        floatFieldInjector.set(1F);
        doubleFieldInjector.set(1D);
        Object value = target.conclude();

        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(value));
        assertThat(record.readUTF("string")).isEqualTo("1");
        assertThat(record.readChar("character")).isEqualTo('1');
        assertThat(record.readBoolean("boolean")).isEqualTo(true);
        assertThat(record.readByte("byte")).isEqualTo((byte) 1);
        assertThat(record.readShort("short")).isEqualTo((short) 1);
        assertThat(record.readInt("int")).isEqualTo(1);
        assertThat(record.readLong("long")).isEqualTo(1L);
        assertThat(record.readFloat("float")).isEqualTo(1F);
        assertThat(record.readDouble("double")).isEqualTo(1D);
    }

    @Test
    public void when_injectNonExistingPropertyValue_then_throws() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector injector = target.createInjector("field");

        target.init();
        assertThatThrownBy(() -> injector.set("1")).isInstanceOf(QueryException.class);
    }

    @Test
    public void when_injectNonExistingPropertyNullValue_then_succeeds() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector injector = target.createInjector("field");

        target.init();
        injector.set(null);
        Object value = target.conclude();

        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(value));
        assertThat(record).isNotNull();
    }
}
