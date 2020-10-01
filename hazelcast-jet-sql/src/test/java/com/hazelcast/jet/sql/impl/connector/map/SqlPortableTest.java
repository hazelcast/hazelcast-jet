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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlPortableTest extends SqlTestSupport {

    private static SqlService sqlService;

    private static final int PERSON_ID_FACTORY_ID = 1;
    private static final int PERSON_ID_CLASS_ID = 2;
    private static final int PERSON_ID_CLASS_VERSION = 3;

    private static final int PERSON_FACTORY_ID = 4;
    private static final int PERSON_CLASS_ID = 5;
    private static final int PERSON_CLASS_VERSION = 6;

    private static final int ALL_TYPES_FACTORY_ID = 7;
    private static final int ALL_TYPES_CLASS_ID = 8;
    private static final int ALL_TYPES_CLASS_VERSION = 9;

    private static InternalSerializationService serializationService;

    @BeforeClass
    // reusing ClassDefinitions as schema does not change
    public static void beforeClass() {
        initialize(1, null);
        sqlService = instance().getSql();

        serializationService = ((HazelcastInstanceImpl) instance().getHazelcastInstance()).getSerializationService();

        ClassDefinition personIdClassDefinition =
                new ClassDefinitionBuilder(PERSON_ID_FACTORY_ID, PERSON_ID_CLASS_ID, PERSON_ID_CLASS_VERSION)
                        .addIntField("id")
                        .build();
        serializationService.getPortableContext().registerClassDefinition(personIdClassDefinition);

        ClassDefinition personClassDefinition =
                new ClassDefinitionBuilder(PERSON_FACTORY_ID, PERSON_CLASS_ID, PERSON_CLASS_VERSION)
                        .addIntField("id")
                        .addUTFField("name")
                        .build();
        serializationService.getPortableContext().registerClassDefinition(personClassDefinition);

        ClassDefinition evolvedPersonClassDefinition =
                new ClassDefinitionBuilder(PERSON_FACTORY_ID, PERSON_CLASS_ID, PERSON_CLASS_VERSION + 1)
                        .addIntField("id")
                        .addUTFField("name")
                        .addLongField("ssn")
                        .build();
        serializationService.getPortableContext().registerClassDefinition(evolvedPersonClassDefinition);

        ClassDefinition allTypesValueClassDefinition =
                new ClassDefinitionBuilder(ALL_TYPES_FACTORY_ID, ALL_TYPES_CLASS_ID, ALL_TYPES_CLASS_VERSION)
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
        serializationService.getPortableContext().registerClassDefinition(allTypesValueClassDefinition);
    }

    @Test
    public void test_insertsIntoDiscoveredMap() {
        String name = generateRandomName();

        instance().getMap(name).put(
                new PortableGenericRecordBuilder(
                        new ClassDefinitionBuilder(PERSON_ID_FACTORY_ID, PERSON_ID_CLASS_ID, PERSON_ID_CLASS_VERSION)
                                .addIntField("id")
                                .build())
                        .writeInt("id", 1)
                        .build(),
                new PortableGenericRecordBuilder(
                        new ClassDefinitionBuilder(PERSON_FACTORY_ID, PERSON_CLASS_ID, PERSON_CLASS_VERSION)
                                .addIntField("id")
                                .addUTFField("name")
                                .build())
                        .writeInt("id", 2)
                        .writeUTF("name", "Alice")
                        .build()
        );

        sqlService.execute("SINK INTO partitioned." + name + " VALUES (2, 'Bob')");

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
    }

    @Test
    public void test_nulls() throws IOException {
        String name = generateRandomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '"' + OPTION_KEY_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_KEY_FACTORY_ID + "\" '" + PERSON_ID_FACTORY_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_ID + "\" '" + PERSON_ID_CLASS_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_VERSION + "\" '" + PERSON_ID_CLASS_VERSION + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FACTORY_ID + "\" '" + PERSON_FACTORY_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_ID + "\" '" + PERSON_CLASS_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_VERSION + "\" '" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + name + " VALUES (null, null)");

        Entry<Data, Data> entry = randomEntryFrom(name);

        InternalGenericRecord keyReader = serializationService.readAsInternalGenericRecord(entry.getKey());
        assertThat(keyReader.readInt("id")).isEqualTo(0);

        InternalGenericRecord valueReader = serializationService.readAsInternalGenericRecord(entry.getValue());
        assertThat(valueReader.readUTF("name")).isNull();

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(0, null))
        );
    }

    @Test
    public void test_fieldsShadowing() throws IOException {
        String name = generateRandomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '"' + OPTION_KEY_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_KEY_FACTORY_ID + "\" '" + PERSON_ID_FACTORY_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_ID + "\" '" + PERSON_ID_CLASS_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_VERSION + "\" '" + PERSON_ID_CLASS_VERSION + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FACTORY_ID + "\" '" + PERSON_FACTORY_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_ID + "\" '" + PERSON_CLASS_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_VERSION + "\" '" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + name + " (id, name) VALUES (1, 'Alice')");

        Entry<Data, Data> entry = randomEntryFrom(name);

        InternalGenericRecord keyReader = serializationService.readAsInternalGenericRecord(entry.getKey());
        assertThat(keyReader.readInt("id")).isEqualTo(1);

        InternalGenericRecord valueReader = serializationService.readAsInternalGenericRecord(entry.getValue());
        assertThat(valueReader.readInt("id")).isEqualTo(0);
        assertThat(valueReader.readUTF("name")).isEqualTo("Alice");

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "Alice"))
        );
    }

    @Test
    public void test_fieldsMapping() throws IOException {
        String name = generateRandomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_id INT EXTERNAL NAME \"__key.id\""
                + ", value_id INT EXTERNAL NAME \"this.id\""
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '"' + OPTION_KEY_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_KEY_FACTORY_ID + "\" '" + PERSON_ID_FACTORY_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_ID + "\" '" + PERSON_ID_CLASS_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_VERSION + "\" '" + PERSON_ID_CLASS_VERSION + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FACTORY_ID + "\" '" + PERSON_FACTORY_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_ID + "\" '" + PERSON_CLASS_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_VERSION + "\" '" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + name + " (value_id, key_id) VALUES (2, 1)");

        Entry<Data, Data> entry = randomEntryFrom(name);

        InternalGenericRecord keyReader = serializationService.readAsInternalGenericRecord(entry.getKey());
        assertThat(keyReader.readInt("id")).isEqualTo(1);

        InternalGenericRecord valueReader = serializationService.readAsInternalGenericRecord(entry.getValue());
        assertThat(valueReader.readInt("id")).isEqualTo(2);

        assertRowsAnyOrder(
                "SELECT key_id, value_id FROM " + name,
                singletonList(new Row(1, 2))
        );
    }

    @Test
    public void test_schemaEvolution() {
        String name = generateRandomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '"' + OPTION_KEY_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_KEY_FACTORY_ID + "\" '" + PERSON_ID_FACTORY_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_ID + "\" '" + PERSON_ID_CLASS_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_VERSION + "\" '" + PERSON_ID_CLASS_VERSION + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FACTORY_ID + "\" '" + PERSON_FACTORY_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_ID + "\" '" + PERSON_CLASS_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_VERSION + "\" '" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice')");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '"' + OPTION_KEY_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_KEY_FACTORY_ID + "\" '" + PERSON_ID_FACTORY_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_ID + "\" '" + PERSON_ID_CLASS_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_VERSION + "\" '" + PERSON_ID_CLASS_VERSION + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FACTORY_ID + "\" '" + PERSON_FACTORY_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_ID + "\" '" + PERSON_CLASS_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_VERSION + "\" '" + (PERSON_CLASS_VERSION + 1) + '\''
                + ")"
        );

        // insert record against new schema/class definition
        sqlService.execute("SINK INTO " + name + " VALUES (2, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(1, "Alice", null),
                        new Row(2, "Bob", 123456789L)
                )
        );
    }

    @Test
    public void test_fieldsExtensions() {
        String name = generateRandomName();
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_KEY_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_KEY_FACTORY_ID + "\" '" + PERSON_ID_FACTORY_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_ID + "\" '" + PERSON_ID_CLASS_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_VERSION + "\" '" + PERSON_ID_CLASS_VERSION + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FACTORY_ID + "\" '" + PERSON_FACTORY_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_ID + "\" '" + PERSON_CLASS_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_VERSION + "\" '" + (PERSON_CLASS_VERSION + 1) + '\''
                + ")"
        );

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice', 123456789)");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " ("
//                + "id INT EXTERNAL NAME \"__key.id\""
                + "name VARCHAR"
                + ", ssn BIGINT"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '"' + OPTION_KEY_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_KEY_FACTORY_ID + "\" '" + PERSON_ID_FACTORY_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_ID + "\" '" + PERSON_ID_CLASS_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_VERSION + "\" '" + PERSON_ID_CLASS_VERSION + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FACTORY_ID + "\" '" + PERSON_FACTORY_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_ID + "\" '" + PERSON_CLASS_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_VERSION + "\" '" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        // insert record against new schema/class definition
        sqlService.execute("SINK INTO " + name + " VALUES ('Bob', null)");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row("Alice", 123456789L),
                        new Row("Bob", null)
                )
        );
    }

    @Test
    public void test_allTypes() throws IOException {
        String name = generateRandomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '"' + OPTION_KEY_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_KEY_FACTORY_ID + "\" '" + PERSON_ID_FACTORY_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_ID + "\" '" + PERSON_ID_CLASS_ID + '\''
                + ", \"" + OPTION_KEY_CLASS_VERSION + "\" '" + PERSON_ID_CLASS_VERSION + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + PORTABLE_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FACTORY_ID + "\" '" + ALL_TYPES_FACTORY_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_ID + "\" '" + ALL_TYPES_CLASS_ID + '\''
                + ", \"" + OPTION_VALUE_CLASS_VERSION + "\" '" + ALL_TYPES_CLASS_VERSION + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + name + " VALUES ("
                + "13"
                + ", 'string'"
                + ", 'a'"
                + ", true"
                + ", 126"
                + ", 32766"
                + ", 2147483646"
                + ", 9223372036854775806"
                + ", 1234567890.1"
                + ", 123451234567890.1"
                + ")"
        );

        InternalGenericRecord allTypesReader = serializationService
                .readAsInternalGenericRecord(randomEntryFrom(name).getValue());
        assertThat(allTypesReader.readUTF("string")).isEqualTo("string");
        assertThat(allTypesReader.readChar("character")).isEqualTo('a');
        assertThat(allTypesReader.readBoolean("boolean")).isTrue();
        assertThat(allTypesReader.readByte("byte")).isEqualTo((byte) 126);
        assertThat(allTypesReader.readShort("short")).isEqualTo((short) 32766);
        assertThat(allTypesReader.readInt("int")).isEqualTo(2147483646);
        assertThat(allTypesReader.readLong("long")).isEqualTo(9223372036854775806L);
        assertThat(allTypesReader.readFloat("float")).isEqualTo(1234567890.1F);
        assertThat(allTypesReader.readDouble("double")).isEqualTo(123451234567890.1D);

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(
                        13,
                        "string",
                        "a",
                        true,
                        (byte) 126,
                        (short) 32766,
                        2147483646,
                        9223372036854775806L,
                        1234567890.1F,
                        123451234567890.1D
                ))
        );
    }

    private static String generateRandomName() {
        return "portable_" + randomString().replace('-', '_');
    }

    @SuppressWarnings({"OptionalGetWithoutIsPresent", "unchecked", "rawtypes"})
    private static Entry<Data, Data> randomEntryFrom(String mapName) {
        NodeEngine engine = ((HazelcastInstanceImpl) instance().getHazelcastInstance()).node.nodeEngine;
        MapService service = engine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = service.getMapServiceContext();

        return Arrays.stream(context.getPartitionContainers())
                     .map(partitionContainer -> partitionContainer.getExistingRecordStore(mapName))
                     .filter(Objects::nonNull)
                     .flatMap(store -> {
                         Iterator<Entry<Data, Record>> iterator = store.iterator();
                         return stream(spliteratorUnknownSize(iterator, ORDERED), false);
                     })
                     .map(entry -> entry(entry.getKey(), (Data) entry.getValue().getValue()))
                     .findFirst()
                     .get();
    }
}
