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
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.CSV_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlCsvTest extends SqlTestSupport {

    private static final String RESOURCES_PATH = Paths.get("src/test/resources").toFile().getAbsolutePath();

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_nulls() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "nonExistingField VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + RESOURCES_PATH + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + "file.csv" + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id TINYINT EXTERNAL NAME byte"
                + ", name VARCHAR EXTERNAL NAME string"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + RESOURCES_PATH + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + "file.csv" + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT id, name FROM " + name,
                singletonList(new Row((byte) 127, "string"))
        );
    }

    @Test
    public void test_allTypes() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "string VARCHAR"
                + ", \"boolean\" BOOLEAN"
                + ", byte TINYINT"
                + ", short SMALLINT"
                + ", \"int\" INT"
                + ", long BIGINT"
                + ", \"float\" REAL"
                + ", \"double\" DOUBLE"
                + ", \"decimal\" DECIMAL"
                + ", \"time\" TIME"
                + ", \"date\" DATE"
                + ", \"timestamp\" TIMESTAMP"
                + ", timestampTz TIMESTAMP WITH TIME ZONE"
                + ", object OBJECT"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + RESOURCES_PATH + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + "file.csv" + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(
                        "string",
                        true,
                        (byte) 127,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1F,
                        123451234567890.1D,
                        new BigDecimal("9223372036854775.123"),
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        null
                ))
        );
    }

    @Test
    public void test_schemaDiscovery() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + RESOURCES_PATH + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + "file.csv" + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT "
                        + "string"
                        + ", \"boolean\""
                        + ", byte"
                        + ", short"
                        + ", \"int\""
                        + ", long"
                        + ", \"float\""
                        + ", \"double\""
                        + ", \"decimal\""
                        + ", \"time\""
                        + ", \"date\""
                        + ", \"timestamp\""
                        + ", \"timestampTz\""
                        + " FROM " + name,
                singletonList(new Row(
                        "string",
                        "true",
                        "127",
                        "32767",
                        "2147483647",
                        "9223372036854775807",
                        "1234567890.1",
                        "123451234567890.1",
                        "9223372036854775.123",
                        "12:23:34",
                        "2020-04-15",
                        "2020-04-15T12:23:34.001",
                        "2020-04-15T12:23:34.200Z"
                ))
        );
    }

    @Test
    public void test_tableFunction() {
        assertRowsAnyOrder(
                "SELECT "
                        + "string"
                        + ", \"boolean\""
                        + ", byte"
                        + ", short"
                        + ", \"int\""
                        + ", long"
                        + ", \"float\""
                        + ", \"double\""
                        + ", \"decimal\""
                        + ", \"time\""
                        + ", \"date\""
                        + ", \"timestamp\""
                        + ", \"timestampTz\""
                        + " FROM TABLE ("
                        + "csv_file ("
                        + " path => '" + RESOURCES_PATH + "'"
                        + " , glob => 'file.csv'"
                        + " , options => MAP['key', 'value']"
                        + ")"
                        + ")",
                singletonList(new Row(
                        "string",
                        "true",
                        "127",
                        "32767",
                        "2147483647",
                        "9223372036854775807",
                        "1234567890.1",
                        "123451234567890.1",
                        "9223372036854775.123",
                        "12:23:34",
                        "2020-04-15",
                        "2020-04-15T12:23:34.001",
                        "2020-04-15T12:23:34.200Z"
                ))
        );
    }

    @Test
    public void test_castTypes() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "string VARCHAR"
                + ", \"boolean\" BOOLEAN"
                + ", byte TINYINT"
                + ", short SMALLINT"
                + ", \"int\" INT"
                + ", long BIGINT"
                + ", \"float\" REAL"
                + ", \"double\" DOUBLE"
                + ", \"decimal\" DECIMAL"
                + ", \"time\" TIME"
                + ", \"date\" DATE"
                + ", \"timestamp\" TIMESTAMP"
                + ", \"timestampTz\" TIMESTAMP WITH TIME ZONE"
                + ") "
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + RESOURCES_PATH + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + "file.csv" + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(
                        "string",
                        true,
                        (byte) 127,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1f,
                        123451234567890.1,
                        new BigDecimal("9223372036854775.123"),
                        LocalTime.parse("12:23:34"),
                        LocalDate.parse("2020-04-15"),
                        LocalDateTime.parse("2020-04-15T12:23:34.001"),
                        OffsetDateTime.parse("2020-04-15T12:23:34.200Z")
                ))
        );
    }

    @Test
    public void when_conversionFails_then_queryFails() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " (string INT) "
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + RESOURCES_PATH + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + "file.csv" + '\''
                + ")"
        );

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " + name).iterator().hasNext())
                .hasMessageEndingWith("Cannot convert VARCHAR to INTEGER");
    }

    @Test
    public void when_columnsSpecified_then_fileNotAccessed() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " (field INT) "
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='/non-existent-directory'"
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + "foo.csv" + '\''
                + ")"
        );
    }
}
