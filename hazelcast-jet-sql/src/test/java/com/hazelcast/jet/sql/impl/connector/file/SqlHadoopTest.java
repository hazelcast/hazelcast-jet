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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
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
import java.time.OffsetDateTime;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.CSV_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSONL_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PARQUET_FORMAT;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SqlHadoopTest extends SqlTestSupport {

    private static MiniDFSCluster cluster;
    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        File directory = Files.createTempDirectory("sql-test-hdfs").toFile().getAbsoluteFile();
        directory.deleteOnExit();

        Configuration configuration = new Configuration();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, directory.getAbsolutePath());
        cluster = new MiniDFSCluster.Builder(configuration).build();
        cluster.waitClusterUp();
    }

    @AfterClass
    public static void afterClass() {
        cluster.shutdown();
    }

    @Test
    public void test_csv() throws IOException {
        store("/csv/file.csv", "id,name\n1,Alice\n2,Bob");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id BIGINT"
                + ", name VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("csv") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", 1L)
                        , new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_csvSchemaDiscovery() throws IOException {
        store("/discovered-csv/file.csv", "id,name\n1,Alice\n2,Bob");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("discovered-csv") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", "1")
                        , new Row("Bob", "2")
                )
        );
    }

    @Test
    public void test_json() throws IOException {
        store("/json/file.json", "{\"id\": 1, \"name\": \"Alice\"}\n{\"id\": 2, \"name\": \"Bob\"}");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id BIGINT"
                + ", name VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + JSONL_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("json") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", 1L)
                        , new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_jsonSchemaDiscovery() throws IOException {
        store("/discovered-json/file.json", "{\"id\": 1, \"name\": \"Alice\"}\n{\"id\": 2, \"name\": \"Bob\"}");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + JSONL_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("discovered-json") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", 1D)
                        , new Row("Bob", 2D)
                )
        );
    }

    @Test
    public void test_avro() throws IOException {
        store("/avro/file.avro", Files.readAllBytes(Paths.get("src/test/resources/file.avro")));

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id BIGINT EXTERNAL NAME long"
                + ", name VARCHAR EXTERNAL NAME string"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("avro") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(9223372036854775807L, "string"))
        );
    }

    @Test
    public void test_avroSchemaDiscovery() throws IOException {
        store("/discovered-avro/file.avro", Files.readAllBytes(Paths.get("src/test/resources/file.avro")));

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("discovered-avro") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT byte, string FROM " + name,
                singletonList(new Row(127, "string"))
        );
    }

    @Test
    public void test_parquet_nulls() throws IOException {
        store("/parquet-nulls/file.parquet", Files.readAllBytes(Paths.get("src/test/resources/file.parquet")));

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "nonExistingField VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + PARQUET_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("parquet-nulls") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void test_parquet_fieldsMapping() throws IOException {
        store("/parquet-fields-mapping/file.parquet", Files.readAllBytes(Paths.get("src/test/resources/file.parquet")));

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id TINYINT EXTERNAL NAME byte"
                + ", name VARCHAR EXTERNAL NAME string"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + PARQUET_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("parquet-fields-mapping") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT id, name FROM " + name,
                singletonList(new Row((byte) 127, "string"))
        );
    }

    @Test
    public void test_parquet_allTypes() throws IOException {
        store("/parquet-all-types/file.parquet", Files.readAllBytes(Paths.get("src/test/resources/file.parquet")));

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
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + PARQUET_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("parquet-all-types") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
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
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC)
                ))
        );
    }

    @Test
    public void test_parquet_schemaDiscovery() throws IOException {
        store("/parquet-schema-discovery/file.parquet", Files.readAllBytes(Paths.get("src/test/resources/file.parquet")));

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + PARQUET_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("parquet-schema-discovery") + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='*'"
                + ")"
        );

        assertRowsAnyOrder("SELECT "
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
                        true,
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1F,
                        123451234567890.1D,
                        "9223372036854775.123",
                        "12:23:34",
                        "2020-04-15",
                        "2020-04-15T12:23:34.001",
                        "2020-04-15T12:23:34.200Z"
                ))
        );
    }

    @Test
    public void test_parquet_tableFunction() throws IOException {
        store("/parquet-table-function/file.parquet", Files.readAllBytes(Paths.get("src/test/resources/file.parquet")));

        assertRowsAnyOrder("SELECT "
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
                        + "PARQUET_FILE ('" + path("parquet-table-function") + "', '*')"
                        + ")",
                singletonList(new Row(
                        "string",
                        true,
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1F,
                        123451234567890.1D,
                        "9223372036854775.123",
                        "12:23:34",
                        "2020-04-15",
                        "2020-04-15T12:23:34.001",
                        "2020-04-15T12:23:34.200Z"
                ))
        );
    }

    private static String path(String suffix) throws IOException {
        return cluster.getFileSystem().getUri() + "/" + suffix;
    }

    private static void store(String path, String content) throws IOException {
        try (FSDataOutputStream output = cluster.getFileSystem().create(new Path(path))) {
            output.writeBytes(content);
        }
    }

    private static void store(String path, byte[] content) throws IOException {
        try (FSDataOutputStream output = cluster.getFileSystem().create(new Path(path))) {
            output.write(content);
        }
    }
}
