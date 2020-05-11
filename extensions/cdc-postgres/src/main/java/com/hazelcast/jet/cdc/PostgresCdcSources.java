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
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Contains factory methods for creating change data capture sources
 * based on PostgreSQL databases.
 *
 * @since 4.2
 */
@EvolvingApi
public final class PostgresCdcSources {

    private PostgresCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a PostgreSQL
     * database to Hazelcast Jet.
     *
     * @param name name of this source, needs to be unique, will be
     *             passed to the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also
     * to construct the source once configuration is done
     */
    @Nonnull
    public static Builder postgres(@Nonnull String name) {
        return new Builder(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a PostgreSQL database to Hazelcast Jet.
     */
    public static final class Builder {

        private static final PropertyRules RULES = new PropertyRules()
                .required("database.hostname")
                .required("database.user")
                .required("database.password")
                .required("database.dbname")
                .required("database.server.name")
                .exclusive("schema.whitelist", "schema.blacklist")
                .exclusive("table.whitelist", "table.blacklist");

        private final DebeziumConfig config;

        /**
         * @param name name of the source, needs to be unique,
         *             will be passed to the underlying Kafka
         *             Connect source
         */
        private Builder(String name) {
            Objects.requireNonNull(name, "name");

            config = new DebeziumConfig(name, "io.debezium.connector.postgresql.PostgresConnector");
        }

        /**
         * IP address or hostname of the database server, has to be
         * specified.
         */
        @Nonnull
        public Builder setDatabaseAddress(@Nonnull String address) {
            config.setProperty("database.hostname", address);
            return this;
        }

        /**
         * Optional port number of the database server, if unspecified
         * defaults to the database specific default port (5432).
         */
        @Nonnull
        public Builder setDatabasePort(int port) {
            config.setProperty("database.port", Integer.toString(port));
            return this;
        }

        /**
         * Database user for connecting to the database server. Has to
         * be specified.
         */
        @Nonnull
        public Builder setDatabaseUser(@Nonnull String user) {
            config.setProperty("database.user", user);
            return this;
        }

        /**
         * Database user password for connecting to the database server.
         * Has to be specified.
         */
        @Nonnull
        public Builder setDatabasePassword(@Nonnull String password) {
            config.setProperty("database.password", password);
            return this;
        }

        /**
         * The name of the PostgreSQL database from which to stream the
         * changes. Has to be set.
         */
        public Builder setDatabaseName(String dbName) {
            config.setProperty("database.dbname", dbName);
            return this;
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
            config.setProperty("database.server.name", cluster);
            return this;
        }

        /**
         * Optional regular expressions that match schema names to be
         * monitored ("schema" is used here to denote logical groups of
         * tables). Any schema name not included in the whitelist will
         * be excluded from monitoring. By default all non-system schemas
         * will be monitored. May not be used with
         * {@link #setSchemaBlacklist(String...) schema blacklist}.
         */
        public Builder setSchemaWhitelist(String... schemaNameRegExps) {
            config.setProperty("schema.whitelist", schemaNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match schema names to be
         * excluded from monitoring ("schema" is used here to denote
         * logical groups of tables). Any schema name not included in
         * the blacklist will be monitored, with the exception of system
         * schemas. May not be used with
         * {@link #setSchemaWhitelist(String...) schema whitelist}.
         */
        public Builder setSchemaBlacklist(String... schemaNameRegExps) {
            config.setProperty("schema.blacklist", schemaNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be monitored; any table not
         * included in the whitelist will be excluded from monitoring.
         * Each identifier is of the form <i>schemaName.tableName</i>.
         * By default the connector will monitor every non-system table
         * in each monitored database. May not be used with
         * {@link #setTableBlacklist(String...) table blacklist}.
         */
        @Nonnull
        public Builder setTableWhitelist(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.whitelist", tableNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any
         * table not included in the blacklist will be monitored. Each
         * identifier is of the form <i>schemaName.tableName</i>. May
         * not be used with
         * {@link #setTableWhitelist(String...) table whitelist}.
         */
        @Nonnull
        public Builder setTableBlacklist(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.blacklist", tableNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match the fully-qualified
         * names of columns that should be excluded from change event
         * message values. Fully-qualified names for columns are of the
         * form <i>schemaName.tableName.columnName</i>.
         */
        @Nonnull
        public Builder setColumnBlacklist(@Nonnull String... columnNameRegExps) {
            config.setProperty("column.blacklist", columnNameRegExps);
            return this;
        }

        /**
         * Can be used to set any property not explicitly covered by
         * other methods or to override properties we have hidden.
         */
        @Nonnull
        public Builder setCustomProperty(@Nonnull String key, @Nonnull String value) {
            config.setProperty(key, value);
            return this;
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        @Nonnull
        public StreamSource<ChangeRecord> build() {
            config.check(RULES);
            return config.createSource();
        }

    }
}
