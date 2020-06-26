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

package com.hazelcast.jet.sql.impl.connector.imap;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.connector.LocalPartitionedMapConnector;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.sql.impl.connector.SqlConnector.PORTABLE_SERIALIZATION_FORMAT;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS_VERSION;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_FACTORY_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS_VERSION;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_FACTORY_ID;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

// TODO: move it to IMDG when INSERTs are supported, or at least move to one of Jet connector tests ?
public class SqlPortableTest extends SqlTestSupport {

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

    private String personMapName;
    private String allTypesMapName;

    @BeforeClass
    // reusing ClassDefinitions as schema does not change
    public static void beforeClass() {
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
                        .addCharField("character0")
                        .addCharField("character1")
                        .addBooleanField("boolean0")
                        .addBooleanField("boolean1")
                        .addByteField("byte0")
                        .addByteField("byte1")
                        .addShortField("short0")
                        .addShortField("short1")
                        .addIntField("int0")
                        .addIntField("int1")
                        .addLongField("long0")
                        .addLongField("long1")
                        .addFloatField("float0")
                        .addFloatField("float1")
                        .addDoubleField("double0")
                        .addDoubleField("double1")
                        .build();
        serializationService.getPortableContext().registerClassDefinition(allTypesValueClassDefinition);
    }

    @Before
    public void before() {
        personMapName = generateRandomName();
        executeSql(format("CREATE EXTERNAL TABLE %s " +
                        "TYPE \"%s\" " +
                        "OPTIONS (" +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'" +
                        ")",
                personMapName, LocalPartitionedMapConnector.TYPE_NAME,
                TO_SERIALIZATION_KEY_FORMAT, PORTABLE_SERIALIZATION_FORMAT,
                TO_KEY_FACTORY_ID, PERSON_ID_FACTORY_ID,
                TO_KEY_CLASS_ID, PERSON_ID_CLASS_ID,
                TO_KEY_CLASS_VERSION, PERSON_ID_CLASS_VERSION,
                TO_SERIALIZATION_VALUE_FORMAT, PORTABLE_SERIALIZATION_FORMAT,
                TO_VALUE_FACTORY_ID, PERSON_FACTORY_ID,
                TO_VALUE_CLASS_ID, PERSON_CLASS_ID,
                TO_VALUE_CLASS_VERSION, PERSON_CLASS_VERSION
        ));

        allTypesMapName = generateRandomName();
        executeSql(format("CREATE EXTERNAL TABLE %s (" +
                        " __key DECIMAL(10, 0)" +
                        ") TYPE \"%s\" " +
                        "OPTIONS (" +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'" +
                        ")",
                allTypesMapName, LocalPartitionedMapConnector.TYPE_NAME,
                TO_SERIALIZATION_VALUE_FORMAT, PORTABLE_SERIALIZATION_FORMAT,
                TO_VALUE_FACTORY_ID, ALL_TYPES_FACTORY_ID,
                TO_VALUE_CLASS_ID, ALL_TYPES_CLASS_ID,
                TO_VALUE_CLASS_VERSION, ALL_TYPES_CLASS_VERSION
        ));
    }

    @Test
    public void supportsNulls() throws IOException {
        executeSql(format("INSERT OVERWRITE %s VALUES (null, null)", personMapName));

        Entry<Data, Data> entry = randomEntryFrom(personMapName);

        PortableReader keyReader = serializationService.createPortableReader(entry.getKey());
        assertThat(keyReader.readInt("id")).isEqualTo(0);

        PortableReader valueReader = serializationService.createPortableReader(entry.getValue());
        assertThat(valueReader.readUTF("name")).isNull();

        assertRowsEventuallyAnyOrder(
                format("SELECT id, name FROM %s", personMapName),
                singletonList(new Row(0, null)));
    }

    @Test
    public void keyShadowsValue() throws IOException {
        executeSql(format("INSERT OVERWRITE %s VALUES (13, 'Alice')", personMapName));

        Entry<Data, Data> entry = randomEntryFrom(personMapName);

        PortableReader keyReader = serializationService.createPortableReader(entry.getKey());
        assertThat(keyReader.readInt("id")).isEqualTo(13);

        PortableReader valueReader = serializationService.createPortableReader(entry.getValue());
        assertThat(valueReader.readUTF("name")).isEqualTo("Alice");

        assertRowsEventuallyAnyOrder(
                format("SELECT * FROM %s", personMapName),
                singletonList(new Row(13, "Alice")));
    }

    @Test
    public void supportsSchemaEvolution() {
        // insert initial record
        executeSql(format("INSERT OVERWRITE %s VALUES (13, 'Alice')", personMapName));

        // alter schema
        executeSql(format("CREATE OR REPLACE EXTERNAL TABLE %s " +
                        "TYPE \"%s\" " +
                        "OPTIONS (" +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'" +
                        ")",
                personMapName, LocalPartitionedMapConnector.TYPE_NAME,
                TO_SERIALIZATION_KEY_FORMAT, PORTABLE_SERIALIZATION_FORMAT,
                TO_KEY_FACTORY_ID, PERSON_ID_FACTORY_ID,
                TO_KEY_CLASS_ID, PERSON_ID_CLASS_ID,
                TO_KEY_CLASS_VERSION, PERSON_ID_CLASS_VERSION,
                TO_SERIALIZATION_VALUE_FORMAT, PORTABLE_SERIALIZATION_FORMAT,
                TO_VALUE_FACTORY_ID, PERSON_FACTORY_ID,
                TO_VALUE_CLASS_ID, PERSON_CLASS_ID,
                TO_VALUE_CLASS_VERSION, PERSON_CLASS_VERSION + 1
        ));
        // insert record against new schema/class definition
        executeSql(format("INSERT OVERWRITE %s VALUES (69, 'Bob', 123456789)", personMapName));

        // assert both - initial & evolved - records are correctly read
        assertRowsEventuallyAnyOrder(
                format("SELECT * FROM %s", personMapName),
                asList(
                        new Row(13, "Alice", null),
                        new Row(69, "Bob", 123456789L)));
    }

    @Test
    public void supportsAllTypes() throws IOException {
        executeSql(format("INSERT OVERWRITE %s VALUES (" +
                "13, --key\n" +
                "'string', --varchar\n" +
                "'a', --character\n" +
                "'b',\n" +
                "true, --boolean\n" +
                "false,\n" +
                "126, --byte\n" +
                "127, \n" +
                "32766, --short\n" +
                "32767, \n" +
                "2147483646, --int \n" +
                "2147483647,\n" +
                "9223372036854775806, --long\n" +
                "9223372036854775807,\n" +
                // TODO: BigDecimal/BigDecimal types when/if supported
                "1234567890.1, --float\n" +
                "1234567890.2, \n" +
                "123451234567890.1, --double\n" +
                "123451234567890.2\n" +
                // TODO: temporal types when/if supported
                ")", allTypesMapName
        ));

        PortableReader allTypesReader = serializationService
                .createPortableReader(randomEntryFrom(allTypesMapName).getValue());
        assertThat(allTypesReader.readUTF("string")).isEqualTo("string");
        assertThat(allTypesReader.readChar("character0")).isEqualTo('a');
        assertThat(allTypesReader.readChar("character1")).isEqualTo('b');
        assertThat(allTypesReader.readBoolean("boolean0")).isTrue();
        assertThat(allTypesReader.readBoolean("boolean1")).isFalse();
        assertThat(allTypesReader.readByte("byte0")).isEqualTo((byte) 126);
        assertThat(allTypesReader.readByte("byte1")).isEqualTo((byte) 127);
        assertThat(allTypesReader.readShort("short0")).isEqualTo((short) 32766);
        assertThat(allTypesReader.readShort("short1")).isEqualTo((short) 32767);
        assertThat(allTypesReader.readInt("int0")).isEqualTo(2147483646);
        assertThat(allTypesReader.readInt("int1")).isEqualTo(2147483647);
        assertThat(allTypesReader.readLong("long0")).isEqualTo(9223372036854775806L);
        assertThat(allTypesReader.readLong("long1")).isEqualTo(9223372036854775807L);
        // TODO: assert BigDecimal/BigDecimal types when/if supported
        assertThat(allTypesReader.readFloat("float0")).isEqualTo(1234567890.1F);
        assertThat(allTypesReader.readFloat("float1")).isEqualTo(1234567890.2F);
        assertThat(allTypesReader.readDouble("double0")).isEqualTo(123451234567890.1D);
        assertThat(allTypesReader.readDouble("double1")).isEqualTo(123451234567890.2D);
        // TODO: assert temporal types when/if supported

        assertRowsEventuallyAnyOrder(
                format("SELECT * FROM %s", allTypesMapName),
                singletonList(new Row(
                        BigDecimal.valueOf(13),
                        "string",
                        "a",
                        "b",
                        true,
                        false,
                        (byte) 126,
                        (byte) 127,
                        (short) 32766,
                        (short) 32767,
                        2147483646,
                        2147483647,
                        9223372036854775806L,
                        9223372036854775807L,
                        // TODO: assert BigDecimal/BigDecimal types when/if supported
                        1234567890.1F,
                        1234567890.2F,
                        123451234567890.1D,
                        123451234567890.2D
                        // TODO: assert temporal types when/if supported
                )));
    }

    private static String generateRandomName() {
        return "m_" + randomString().replace('-', '_');
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
