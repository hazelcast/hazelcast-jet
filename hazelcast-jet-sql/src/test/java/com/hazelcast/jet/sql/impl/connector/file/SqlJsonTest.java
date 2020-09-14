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

import com.hazelcast.jet.sql.JetSqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.AllTypesSqlConnector;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_FORMAT;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;

public class SqlJsonTest extends JetSqlTestSupport {

    private static final String RESOURCES_PATH = Paths.get("src/test/resources").toFile().getAbsolutePath();

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void supportsNulls() {
        String name = createRandomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "nonExistingField VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + RESOURCES_PATH + "'"
                + ", " + FileSqlConnector.OPTION_GLOB + " '" + "file.json" + "'"
                + ")"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void supportsFieldsMapping() {
        String name = createRandomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "name VARCHAR EXTERNAL NAME string"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + RESOURCES_PATH + "'"
                + ", " + FileSqlConnector.OPTION_GLOB + " '" + "file.json" + "'"
                + ")"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT name FROM " + name,
                singletonList(new Row("string"))
        );
    }

    @Test
    @SuppressWarnings("checkstyle:LineLength")
    public void supportsAllTypes() throws IOException {
        File directory = Files.createTempDirectory("sql-test-local-json").toFile();
        directory.deleteOnExit();

        String from = createRandomName();
        sqlService.execute("CREATE MAPPING " + from + ' '
                + "TYPE " + AllTypesSqlConnector.TYPE_NAME
        );

        String to = createRandomName();
        sqlService.execute("CREATE MAPPING " + to + " ("
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
                + ") TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + directory.getAbsolutePath() + "'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + to + " SELECT "
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
                + " FROM " + from
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + to,
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
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC).withZoneSameInstant(systemDefault()).toOffsetDateTime()
                ))
        );
    }

    @Test
    public void supportsSchemaDiscovery() {
        String name = createRandomName();
        sqlService.execute("CREATE MAPPING " + name + " "
                + "TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + RESOURCES_PATH + "'"
                + ", " + FileSqlConnector.OPTION_GLOB + " '" + "file.json" + "'"
                + ")"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT string, \"boolean\", long, \"double\", \"null\" FROM " + name,
                singletonList(new Row(
                        "string"
                        , true
                        , 123456789.0
                        , 123456789.1
                        , null
                ))
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT 1 FROM " + name + " WHERE object IS NOT NULL",
                singletonList(new Row((byte) 1))
        );
    }

    private static String createRandomName() {
        return "json_" + randomString().replace('-', '_');
    }
}
