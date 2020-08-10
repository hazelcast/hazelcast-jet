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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.jet.sql.SqlTestSupport;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.jet.sql.SqlConnector.AVRO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.SqlConnector.OPTION_SERIALIZATION_FORMAT;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;

public class SqlAvroTest extends SqlTestSupport {

    @Test
    public void supportsNulls() {
        String name = createRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "nonExistingField VARCHAR"
                + ") TYPE \"" + FileSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + OPTION_SERIALIZATION_FORMAT + "\" '" + AVRO_SERIALIZATION_FORMAT + "'"
                + ", \"" + FileSqlConnector.OPTION_PATH + "\" '" + RESOURCES_PATH + "'"
                + ", \"" + FileSqlConnector.OPTION_GLOB + "\" '" + "all-types.avro" + "'"
                + ")"
        );

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void supportsFieldsMapping() {
        String name = createRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "name VARCHAR EXTERNAL NAME string"
                + ") TYPE \"" + FileSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + OPTION_SERIALIZATION_FORMAT + "\" '" + AVRO_SERIALIZATION_FORMAT + "'"
                + ", \"" + FileSqlConnector.OPTION_PATH + "\" '" + RESOURCES_PATH + "'"
                + ", \"" + FileSqlConnector.OPTION_GLOB + "\" '" + "all-types.avro" + "'"
                + ")"
        );

        assertRowsEventuallyAnyOrder(
                "SELECT name FROM " + name,
                singletonList(new Row("string"))
        );
    }

    @Test
    public void supportsAllTypes() {
        String name = createRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "string VARCHAR"
                + ", \"boolean\" BOOLEAN"
                + ", byte TINYINT"
                + ", short SMALLINT"
                + ", \"int\" INT"
                + ", long BIGINT"
                + ", \"float\" REAL"
                + ", \"double\" DOUBLE"
                + ", \"bigDecimal\" DECIMAL"
                + ", \"localTime\" TIME"
                + ", \"localDate\" DATE"
                + ", \"localDateTime\" TIMESTAMP"
                + ", offsetDateTime TIMESTAMP WITH TIME ZONE"
                + ") TYPE \"" + FileSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + OPTION_SERIALIZATION_FORMAT + "\" '" + AVRO_SERIALIZATION_FORMAT + "'"
                + ", \"" + FileSqlConnector.OPTION_PATH + "\" '" + RESOURCES_PATH + "'"
                + ", \"" + FileSqlConnector.OPTION_GLOB + "\" '" + "all-types.avro" + "'"
                + ")"
        );

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(
                        "string"
                        , true
                        , (byte) 126
                        , (short) 32766
                        , 2147483646
                        , 9223372036854775806L
                        , 1234567890.1F
                        , 123451234567890.1
                        , new BigDecimal("9223372036854775.111")
                        , LocalTime.of(12, 23, 34)
                        , LocalDate.of(2020, 7, 1)
                        , LocalDateTime.of(2020, 7, 1, 12, 23, 34, 100_000_000)
                        , OffsetDateTime.of(2020, 7, 1, 12, 23, 34, 600_000_000, UTC)
                ))
        );
    }

    @Test
    public void supportsSchemaDiscovery() {
        String name = createRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " "
                + "TYPE \"" + FileSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ( "
                + "\"" + OPTION_SERIALIZATION_FORMAT + "\" '" + AVRO_SERIALIZATION_FORMAT + "'"
                + ", \"" + FileSqlConnector.OPTION_PATH + "\" '" + RESOURCES_PATH + "'"
                + ", \"" + FileSqlConnector.OPTION_GLOB + "\" '" + "file.avro" + "'"
                + ")"
        );

        assertRowsEventuallyAnyOrder(
                "SELECT string, \"boolean\", \"int\", long, \"float\", \"double\", \"null\" FROM " + name,
                singletonList(new Row(
                        "string"
                        , true
                        , 2147483646
                        , 9223372036854775806L
                        , 1234567890.1F
                        , 123451234567890.1
                        , null
                ))
        );

        assertRowsEventuallyAnyOrder(
                "SELECT 1 FROM " + name + " WHERE object IS NOT NULL",
                singletonList(new Row((byte) 1))
        );
    }

    private static String createRandomName() {
        return "avro_" + randomString().replace('-', '_');
    }
}
