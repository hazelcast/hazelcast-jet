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
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.CSV_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_HEADER;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SqlHadoopTest extends JetSqlTestSupport {

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
    public void supportsCsv() throws IOException {
        String name = createRandomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id BIGINT"
                + ", name VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_FORMAT + " '" + CSV_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + path("csv") + "'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + name + " VALUES (1, 'Alice')");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1L, "Alice"))
        );
    }

    @Test
    public void supportsCsvSchemaDiscovery() throws IOException {
        store("/inferred-csv/users.csv", "id,name\n1,Alice\n2,Bob");

        String name = createRandomName();
        sqlService.execute("CREATE MAPPING " + name + " "
                + "TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_FORMAT + " '" + CSV_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + path("inferred-csv") + "'"
                + ", " + OPTION_HEADER + " '" + Boolean.TRUE + "'"
                + ")"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", "1")
                        , new Row("Bob", "2")
                )
        );
    }

    @Test
    public void supportsJson() throws IOException {
        String name = createRandomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id BIGINT"
                + ", name VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + path("json") + "'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + name + " VALUES (1, 'Alice')");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1L, "Alice"))
        );
    }

    @Test
    public void supportsJsonSchemaDiscovery() throws IOException {
        store("/inferred-json/users.csv",
                "{\"id\": \"1\", \"name\": \"Alice\"}\n"
                        + "{\"id\": \"2\", \"name\": \"Bob\"}"
        );

        String name = createRandomName();
        sqlService.execute("CREATE MAPPING " + name + " "
                + "TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + path("inferred-json") + "'"
                + ")"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", "1")
                        , new Row("Bob", "2")
                )
        );
    }

    @Test
    public void supportsAvro() throws IOException {
        String name = createRandomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id BIGINT"
                + ", name VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_FORMAT + " '" + AVRO_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + path("avro") + "'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + name + " VALUES (1, 'Alice')");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1L, "Alice"))
        );
    }

    @Test
    public void supportsAvroSchemaDiscovery() throws IOException {
        store("/inferred-avro/users.avro", Files.readAllBytes(Paths.get("src/test/resources/users.avro")));

        String name = createRandomName();
        sqlService.execute("CREATE MAPPING " + name + " "
                + "TYPE " + FileSqlConnector.TYPE_NAME + " "
                + "OPTIONS ("
                + OPTION_SERIALIZATION_FORMAT + " '" + AVRO_SERIALIZATION_FORMAT + "'"
                + ", " + FileSqlConnector.OPTION_PATH + " '" + path("inferred-avro") + "'"
                + ")"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT username, age FROM " + name,
                asList(
                        new Row("User0", 0)
                        , new Row("User1", 1)
                )
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

    private static String createRandomName() {
        return "hadoop_" + randomString().replace('-', '_');
    }
}
