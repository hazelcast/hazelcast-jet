/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.impl.AbstractSourceBuilder;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for creating change data capture sources
 * based on MySQL databases.
 *
 * @since 4.2
 */
@EvolvingApi
public final class MySqlCdcSources {

    private MySqlCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a MySQL
     * database to Hazelcast Jet.
     *
     * @param name name of this source, needs to be unique, will be
     *             passed to the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also
     * to construct the source once configuration is done
     */
    @Nonnull
    public static Builder mysql(@Nonnull String name) {
        return new Builder(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a MySQL database to Hazelcast Jet.
     */
    public static final class Builder extends AbstractSourceBuilder<Builder> {

        private static final PropertyRules RULES = new PropertyRules()
                .required("database.hostname")
                .required("database.user")
                .required("database.password")
                .required("database.server.name")
                .exclusive("database.whitelist", "database.blacklist")
                .exclusive("table.whitelist", "table.blacklist");


        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        private Builder(String name) {
            super(name, "io.debezium.connector.mysql.MySqlConnector");
            properties.put("include.schema.changes", "false");
        }

        /**
         * IP address or hostname of the database server, has to be
         * specified.
         */
        @Nonnull
        public Builder setDatabaseAddress(@Nonnull String address) {
            return setProperty("database.hostname", address);
        }

        /**
         * Optional port number of the database server, if unspecified
         * defaults to the database specific default port (3306).
         */
        @Nonnull
        public Builder setDatabasePort(int port) {
            return setProperty("database.port", Integer.toString(port));
        }

        /**
         * Database user for connecting to the database server. Has to
         * be specified.
         */
        @Nonnull
        public Builder setDatabaseUser(@Nonnull String user) {
            return setProperty("database.user", user);
        }

        /**
         * Database user password for connecting to the database server.
         * Has to be specified.
         */
        @Nonnull
        public Builder setDatabasePassword(@Nonnull String password) {
            return setProperty("database.password", password);
        }

        /**
         * Logical name that identifies and provides a namespace for the
         * particular database server/cluster being monitored. The
         * logical name should be unique across all other connectors.
         * Only alphanumeric characters and underscores should be used.
         * Has to be specified.
         */
        @Nonnull
        public Builder setClusterName(@Nonnull String cluster) {
            return setProperty("database.server.name", cluster);
        }

        /**
         * A numeric ID of this database client, which must be unique
         * across all currently-running database processes in the MySQL
         * cluster. This connector joins the MySQL database cluster as
         * another server (with this unique ID) so it can read the
         * binlog. By default, a random number is generated between
         * 5400 and 6400, though we recommend setting an explicit value.
         */
        @Nonnull
        public Builder setDatabaseClientId(int clientId) {
            return setProperty("database.server.id", clientId);
        }

        /**
         * Optional regular expressions that match database names to be
         * monitored; any database name not included in the whitelist
         * will be excluded from monitoring. By default all databases
         * will be monitored. May not be used with
         * {@link #setDatabaseBlacklist(String...) database blacklist}.
         */
        @Nonnull
        public Builder setDatabaseWhitelist(@Nonnull String... dbNameRegExps) {
            return setProperty("database.whitelist", dbNameRegExps);
        }

        /**
         * Optional regular expressions that match database names to be
         * excluded from monitoring; any database name not included in
         * the blacklist will be monitored. May not be used with
         * {@link #setDatabaseWhitelist(String...) database whitelist}.
         */
        @Nonnull
        public Builder setDatabaseBlacklist(@Nonnull String... dbNameRegExps) {
            return setProperty("database.blacklist", dbNameRegExps);
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be monitored; any table not
         * included in the whitelist will be excluded from monitoring.
         * Each identifier is of the form <i>databaseName.tableName</i>.
         * By default the connector will monitor every non-system table
         * in each monitored database. May not be used with
         * {@link #setTableBlacklist(String...) table blacklist}.
         */
        @Nonnull
        public Builder setTableWhitelist(@Nonnull String... tableNameRegExps) {
            return setProperty("table.whitelist", tableNameRegExps);
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any
         * table not included in the blacklist will be monitored. Each
         * identifier is of the form <i>databaseName.tableName</i>. May
         * not be used with
         * {@link #setTableWhitelist(String...) table whitelist}.
         */
        @Nonnull
        public Builder setTableBlacklist(@Nonnull String... tableNameRegExps) {
            return setProperty("table.blacklist", tableNameRegExps);
        }

        /**
         * Optional regular expressions that match the fully-qualified
         * names of columns that should be excluded from change event
         * message values. Fully-qualified names for columns are of the
         * form <i>databaseName.tableName.columnName</i>, or
         * <i>databaseName.schemaName.tableName.columnName</i>.
         */
        @Nonnull
        public Builder setColumnBlacklist(@Nonnull String... columnNameRegExps) {
            return setProperty("column.blacklist", columnNameRegExps);
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        @Nonnull
        public StreamSource<ChangeEvent> build() {
            RULES.check(properties);
            return connect(properties);
        }

    }
}
