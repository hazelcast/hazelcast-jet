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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.jet.sql.JetSqlConnector.AVRO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.JetSqlConnector.TO_SERIALIZATION_FORMAT;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;

public class SqlAvroTest extends SqlTestSupport {

    @Test
    public void supportsFieldsMapping() throws IOException {
        String name = createRandomName();
        executeSql(format("CREATE EXTERNAL TABLE %s (" +
                        " name VARCHAR EXTERNAL NAME string" +
                        ") TYPE \"%s\" " +
                        "OPTIONS (" +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'" +
                        ")",
                name, FileSqlConnector.TYPE_NAME,
                FileSqlConnector.TO_DIRECTORY, Paths.get("src/test/resources").toFile().getCanonicalPath(),
                FileSqlConnector.TO_GLOB, "all-types.avro",
                TO_SERIALIZATION_FORMAT, AVRO_SERIALIZATION_FORMAT
        ));

        assertRowsEventuallyAnyOrder(
                format("SELECT name FROM %s", name),
                singletonList(new Row("string"))
        );
    }

    @Test
    public void supportsAllTypes() throws IOException {
        String name = createRandomName();
        executeSql(format("CREATE EXTERNAL TABLE %s (" +
                        " string VARCHAR," +
                        " \"character\" CHAR," +
                        " \"boolean\" BOOLEAN," +
                        " \"byte\" TINYINT," +
                        " short SMALLINT," +
                        " \"int\" INT, " +
                        " long BIGINT," +
                        " bigDecimal DEC(10, 1)," +
                        " bigInteger NUMERIC(5, 0)," +
                        " \"float\" REAL," +
                        " \"double\" DOUBLE," +
                        " \"localTime\" TIME," +
                        " localDate DATE," +
                        " localDateTime TIMESTAMP," +
                        " \"date\" TIMESTAMP WITH LOCAL TIME ZONE (\"DATE\")," +
                        " calendar TIMESTAMP WITH TIME ZONE (\"CALENDAR\")," +
                        " instant TIMESTAMP WITH LOCAL TIME ZONE," +
                        " zonedDateTime TIMESTAMP WITH TIME ZONE (\"ZONED_DATE_TIME\")," +
                        " offsetDateTime TIMESTAMP WITH TIME ZONE" +
                        ") TYPE \"%s\" " +
                        "OPTIONS (" +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'" +
                        ")",
                name, FileSqlConnector.TYPE_NAME,
                FileSqlConnector.TO_DIRECTORY, Paths.get("src/test/resources").toFile().getCanonicalPath(),
                FileSqlConnector.TO_GLOB, "all-types.avro",
                TO_SERIALIZATION_FORMAT, AVRO_SERIALIZATION_FORMAT
        ));

        assertRowsEventuallyAnyOrder(
                format("SELECT * FROM %s", name),
                singletonList(new Row(
                        "string",
                        "a",
                        true,
                        (byte) 126,
                        (short) 32766,
                        2147483646,
                        9223372036854775806L,
                        new BigDecimal("9223372036854775.111"),
                        new BigDecimal("9223372036854775222"),
                        1234567890.1f,
                        123451234567890.1,
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 7, 1),
                        LocalDateTime.of(2020, 7, 1, 12, 23, 34, 100_000_000),
                        OffsetDateTime.of(2020, 7, 1, 12, 23, 34, 200_000_000, UTC),
                        OffsetDateTime.of(2020, 7, 1, 12, 23, 34, 300_000_000, UTC),
                        OffsetDateTime.of(2020, 7, 1, 12, 23, 34, 400_000_000, UTC),
                        OffsetDateTime.of(2020, 7, 1, 12, 23, 34, 500_000_000, UTC),
                        OffsetDateTime.of(2020, 7, 1, 12, 23, 34, 600_000_000, UTC)
                ))
        );
    }

    private static String createRandomName() {
        return "avro_" + randomString().replace('-', '_');
    }
}
