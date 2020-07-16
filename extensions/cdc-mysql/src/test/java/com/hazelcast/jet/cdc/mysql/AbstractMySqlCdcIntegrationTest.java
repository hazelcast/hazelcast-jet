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

package com.hazelcast.jet.cdc.mysql;

import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import org.junit.Rule;
import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public abstract class AbstractMySqlCdcIntegrationTest extends AbstractCdcIntegrationTest {

    @Rule
    public MySQLContainer<?> mysql = new MySQLContainer<>("debezium/example-mysql:1.2")
            .withUsername("mysqluser")
            .withPassword("mysqlpw");

    protected MySqlCdcSources.Builder sourceBuilder(String name) {
        return MySqlCdcSources.mysql(name)
                .setDatabaseAddress(mysql.getContainerIpAddress())
                .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1");
    }

    protected void createDb(String database) throws SQLException {
        String jdbcUrl = "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(MYSQL_PORT) + "/";
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "root", "mysqlpw")) {
            Statement statement = connection.createStatement();
            statement.addBatch("CREATE DATABASE " + database);
            statement.addBatch("GRANT ALL PRIVILEGES ON " + database + ".* TO 'mysqluser'@'%'");
            statement.executeBatch();
        }
    }

}
