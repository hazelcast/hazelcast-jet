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

import static com.hazelcast.jet.sql.SqlConnector.AVRO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.SqlConnector.CSV_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.SqlConnector.JSON_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.SqlConnector.TO_SERIALIZATION_FORMAT;
import static java.util.Arrays.asList;

public class SqlHadoopTest extends SqlTestSupport {

    private static MiniDFSCluster cluster;

    @BeforeClass
    public static void beforeClass() throws IOException {
        File directory = Files.createTempDirectory("test_sql_hdfs").toFile().getAbsoluteFile();
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
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "firstName VARCHAR"
                + ", lastName VARCHAR"
                + ") TYPE \"" + FileSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + FileSqlConnector.TO_PATH + "\" '" + cluster.getFileSystem().getUri() + "/csv/" + "'"
                + ", \"" + TO_SERIALIZATION_FORMAT + "\" '" + CSV_SERIALIZATION_FORMAT + "'"
                + ")"
        );

        store("/csv/users-1.csv", "Joe,Doe");
        store("/csv/users-2.csv", "Alice,Smith\nBob,Unknown");

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row("Joe", "Doe")
                        , new Row("Alice", "Smith")
                        , new Row("Bob", "Unknown")
                )
        );
    }

    @Test
    public void supportsJson() throws IOException {
        String name = createRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "firstName VARCHAR"
                + ", lastName VARCHAR"
                + ") TYPE \"" + FileSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + FileSqlConnector.TO_PATH + "\" '" + cluster.getFileSystem().getUri() + "/json/" + "'"
                + ", \"" + TO_SERIALIZATION_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ")"
        );

        store("/json/users-1.csv", "{\"firstName\": \"Joe\", \"lastName\": \"Doe\"}");
        store("/json/users-2.csv",
                "{\"firstName\": \"Alice\", \"lastName\": \"Smith\"}\n" +
                        "{\"firstName\": \"Bob\", \"lastName\": \"Unknown\"}"
        );

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row("Joe", "Doe")
                        , new Row("Alice", "Smith")
                        , new Row("Bob", "Unknown")
                )
        );
    }

    @Test
    public void supportsAvro() throws IOException {
        String name = createRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "age INT"
                + ", username VARCHAR"
                + ") TYPE \"" + FileSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + FileSqlConnector.TO_PATH + "\" '" + cluster.getFileSystem().getUri() + "/avro/" + "'"
                + ", \"" + TO_SERIALIZATION_FORMAT + "\" '" + AVRO_SERIALIZATION_FORMAT + "'"
                + ")"
        );

        store("/avro/users-1.avro", Files.readAllBytes(Paths.get("src/test/resources/users.avro")));

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(0, "User0")
                        , new Row(1, "User1")
                )
        );
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
