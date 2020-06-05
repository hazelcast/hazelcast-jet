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

package com.hazelcast.jet.cdc.postgres;

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.impl.CdcSource;
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.cdc.postgres.impl.PostgresSequenceExtractor;
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
            config.setProperty(CdcSource.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, PostgresSequenceExtractor.class.getName());
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
         * The name of the @see <a href="https://www.postgresql.org/docs/10/logicaldecoding.html">
         * Postgres logical decoding plug-in</a> installed on the server.
         * Supported values are <i>decoderbufs</i>, <i>wal2json</i>,
         * <i>wal2json_rds</i>, <i>wal2json_streaming</i>,
         * <i>wal2json_rds_streaming</i> and <i>pgoutput</i>.
         * <p>
         * If not explicitly set, the property defaults to <i>decoderbufs</i>.
         * <p>
         * When the processed transactions are very large it is possible
         * that the JSON batch event with all changes in the transaction
         * will not fit into the hard-coded memory buffer of size 1 GB.
         * In such cases it is possible to switch to so-called streaming
         * mode when every change in transactions is sent as a separate
         * message from PostgreSQL.
         */
        @Nonnull
        public Builder setLogicalDecodingPlugIn(@Nonnull String pluginName) {
            config.setProperty("plugin.name", pluginName);
            return this;
        }

        /**
         * The name of the @see <a href="https://www.postgresql.org/docs/10/logicaldecoding-explanation.html">
         * Postgres logical decoding slot</a> (also called "replication
         * slot") created for streaming changes from a plug-in and
         * database instance.
         * <p>
         * Values must conform to Postgres replication slot naming rules
         * which state: "Each replication slot has a name, which can
         * contain lower-case letters, numbers, and the underscore
         * character."
         * <p>
         * Replication slots have to have an identifier that is unique
         * across all databases in a PostgreSQL cluster.
         * <p>
         * If not explicitly set, the property defaults to <i>debezium</i>.
         */
        @Nonnull
        public Builder setReplicationSlotName(@Nonnull String slotName) {
            config.setProperty("slot.name", slotName);
            return this;
        }

        /**
         * Whether or not to drop the logical replication slot when the
         * connector disconnects cleanly.
         * <p>
         * Defaults to <i>false</i>
         * <p>
         * Should only be set to <i>true</i> in testing or development
         * environments. Dropping the slot allows WAL segments to be
         * discarded by the database, so it may happen that after a
         * restart the connector cannot resume from the WAL position
         * where it left off before.
         */
        @Nonnull
        public Builder setReplicationSlotDropOnStop(boolean dropOnStop) {
            config.setProperty("slot.drop.on.stop", dropOnStop);
            return this;
        }

        /**
         * The name of the <a href="https://www.postgresql.org/docs/10/logical-replication-publication.html">
         * Postgres publication</a> that will be used for CDC purposes.
         * <p>
         * If the publication does not exist when this source starts up,
         * then the source will create it (note: the database user of the
         * source must have superuser permissions to be able to do so).
         * If created this way the publication will include all tables
         * and the source itself must filter the data based on its
         * white-/blacklist configs. This is not efficient because the
         * database will still send all data to the connector, before
         * filtering is applied.
         * <p>
         * It's best to use a pre-defined publication (via the <code>CREATE
         * PUBLICATION</code> SQL command, specified via its name.
         * <p>
         * If not explicitly set, the property defaults to <i>dbz_publication</i>.
         */
        @Nonnull
        public Builder setPublicationName(@Nonnull String publicationName) {
            config.setProperty("publication.name", publicationName);
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
