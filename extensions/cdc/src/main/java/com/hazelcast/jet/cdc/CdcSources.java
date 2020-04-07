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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.impl.ChangeEventJsonImpl;
import com.hazelcast.jet.cdc.impl.ChangeEventMongoImpl;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.contrib.connect.impl.AbstractKafkaConnectSource;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import java.util.Properties;

/**
 * Contains factory methods for creating change data capture sources
 *
 * @since 4.1
 */
@EvolvingApi
public final class CdcSources {

    //todo: test with actual cluster

    //todo: can we use these sources in a distributed way?

    //todo: update main README.md file in cdc module

    //todo: the ObjectMapper, even if it's serializable, shouldn't be in each event!

    //todo: sources could have an optional, custom ObjectMapper param, which would simplify the definition of data objects

    private CdcSources() {
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
    public static MySqlBuilder mysql(String name) {
        return new MySqlBuilder(name);
    }

    /**
     * Creates a CDC source that streams change data from a PostgreSQL
     * database to Hazelcast Jet.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also
     * to construct the source once configuration is done
     */
    public static PostgresBuilder postgres(String name) {
        return new PostgresBuilder(name);
    }

    /**
     * Creates a CDC source that streams change data from an SQL Server
     * database to Hazelcast Jet.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also
     * to construct the source once configuration is done
     */
    public static SqlServerBuilder sqlserver(String name) {
        return new SqlServerBuilder(name);
    }

    /**
     * Creates a CDC source that streams change data from a MongoDB
     * database to Hazelcast Jet.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also
     * to construct the source once configuration is done
     */
    public static MongoBuilder mongodb(String name) {
        return new MongoBuilder(name);
    }

    /**
     * Creates a CDC source that streams change data from your Debezium
     * supported database to the Hazelcast Jet pipeline.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also
     * to construct the source once configuration is done
     */
    public static DebeziumBuilder debezium(String name, String connectorClass) {
        return new DebeziumBuilder(name, connectorClass);
    }

    private static StreamSource<ChangeEvent> connect(
            @Nonnull Properties properties,
            @Nonnull SupplierEx<ObjectMapper> objectMapperSupplier,
            @Nonnull BiFunctionEx<ObjectMapper, ChangeEvent, Long> eventToTimestampMapper,
            @Nonnull BiFunctionEx<ObjectMapper, SourceRecord, ChangeEvent> recordToEventMapper) {
        String name = properties.getProperty("name");
        FunctionEx<Processor.Context, KafkaConnectSource> createFn = ctx -> new KafkaConnectSource(ctx, properties,
                objectMapperSupplier, recordToEventMapper, eventToTimestampMapper);
        return SourceBuilder.timestampedStream(name, createFn)
                .fillBufferFn(KafkaConnectSource::fillBuffer)
                .createSnapshotFn(KafkaConnectSource::createSnapshot)
                .restoreSnapshotFn(KafkaConnectSource::restoreSnapshot)
                .destroyFn(KafkaConnectSource::destroy)
                .build();
    }

    private static ObjectMapper createObjectMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private abstract static class AbstractBuilder<SELF extends AbstractBuilder<SELF>> {

        protected final Properties properties = new Properties();

        /**
         * @param name           name of the source, needs to be unique,
         *                       will be passed to the underlying Kafka
         *                       Connect source
         * @param connectorClass name of the Java class for the connector,
         *                       hardcoded for each type of DB
         */
        private AbstractBuilder(String name, String connectorClass) {
            properties.put("name", name);
            properties.put("connector.class", connectorClass);
            properties.put("database.history", HazelcastListDatabaseHistory.class.getName());
            properties.put("database.history.hazelcast.list.name", name);
            properties.put("tombstones.on.delete", "false");
        }

        /**
         * Can be used to set any property not covered by our builders,
         * or to override properties we have hidden.
         *
         * @param key   the name of the property to set
         * @param value the value of the property to set
         * @return the builder itself
         */
        public SELF setCustomProperty(String key, String value) {
            properties.put(key, value);
            return (SELF) this;
        }

        protected SELF setProperty(String key, String value) {
            properties.put(key, value);
            return (SELF) this;
        }

        protected SELF setProperty(String key, int value) {
            return setProperty(key, Integer.toString(value));
        }

        protected SELF setProperty(String key, boolean value) {
            return setProperty(key, Boolean.toString(value));
        }

        protected SELF setProperty(String key, String... values) {
            return setProperty(key, String.join(",", values));
        }
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a MySQL database to Hazelcast Jet.
     */
    public static final class MySqlBuilder extends AbstractBuilder<MySqlBuilder> {

        private static final PropertyRules RULES = new PropertyRules()
                .mandatory("database.hostname")
                .mandatory("database.user")
                .mandatory("database.password")
                .mandatory("database.server.name")
                .exclusive("database.whitelist", "database.blacklist")
                .exclusive("table.whitelist", "table.blacklist");


        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        private MySqlBuilder(String name) {
            super(name, "io.debezium.connector.mysql.MySqlConnector");
            properties.put("include.schema.changes", "false");
        }

        /**
         * IP address or hostname of the database server, has to be
         * specified.
         */
        public MySqlBuilder setDatabaseAddress(String address) {
            return setProperty("database.hostname", address);
        }

        /**
         * Optional port number of the database server, if unspecified
         * defaults to the database specific default port (3306).
         */
        public MySqlBuilder setDatabasePort(int port) {
            return setProperty("database.port", Integer.toString(port));
        }

        /**
         * Database user for connecting to the database server. Has to
         * be specified.
         */
        public MySqlBuilder setDatabaseUser(String user) {
            return setProperty("database.user", user);
        }

        /**
         * Database user password for connecting to the database server.
         * Has to be specified.
         */
        public MySqlBuilder setDatabasePassword(String password) {
            return setProperty("database.password", password);
        }

        /**
         * Logical name that identifies and provides a namespace for the
         * particular database server/cluster being monitored. The
         * logical name should be unique across all other connectors.
         * Only alphanumeric characters and underscores should be used.
         * Has to be specified.
         */
        public MySqlBuilder setClusterName(String cluster) {
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
        public MySqlBuilder setDatabaseClientId(int clientId) {
            return setProperty("database.server.id", clientId);
        }

        /**
         * Optional regular expressions that match database names to be
         * monitored; any database name not included in the whitelist
         * will be excluded from monitoring. By default all databases
         * will be monitored. May not be used with
         * {@link #setDatabaseBlacklist(String...) database blacklist}.
         */
        public MySqlBuilder setDatabaseWhitelist(String... dbNameRegExps) {
            return setProperty("database.whitelist", dbNameRegExps);
        }

        /**
         * Optional regular expressions that match database names to be
         * excluded from monitoring; any database name not included in
         * the blacklist will be monitored. May not be used with
         * {@link #setDatabaseWhitelist(String...) database whitelist}.
         */
        public MySqlBuilder setDatabaseBlacklist(String... dbNameRegExps) {
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
        public MySqlBuilder setTableWhitelist(String... tableNameRegExps) {
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
        public MySqlBuilder setTableBlacklist(String... tableNameRegExps) {
            return setProperty("table.blacklist", tableNameRegExps);
        }

        /**
         * Optional regular expressions that match the fully-qualified
         * names of columns that should be excluded from change event
         * message values. Fully-qualified names for columns are of the
         * form <i>databaseName.tableName.columnName</i>, or
         * <i>databaseName.schemaName.tableName.columnName</i>.
         */
        public MySqlBuilder setColumnBlacklist(String... columnNameRegExps) {
            return setProperty("column.blacklist", columnNameRegExps);
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
            RULES.check(properties);
            return connect(properties,
                    CdcSources::createObjectMapper,
                    (mapper, event) -> event.timestamp(),
                    (mapper, record) -> {
                        String keyJson = Values.convertToString(record.keySchema(), record.key());
                        String valueJson = Values.convertToString(record.valueSchema(), record.value());
                        return new ChangeEventJsonImpl(keyJson, valueJson, mapper);
                    }
            );
        }

    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a PostgreSQL database to Hazelcast Jet.
     */
    public static final class PostgresBuilder extends AbstractBuilder<PostgresBuilder> {

        private static final PropertyRules RULES = new PropertyRules()
                .mandatory("database.hostname")
                .mandatory("database.user")
                .mandatory("database.password")
                .mandatory("database.dbname")
                .mandatory("database.server.name")
                .exclusive("schema.whitelist", "schema.blacklist")
                .exclusive("table.whitelist", "table.blacklist");

        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        private PostgresBuilder(String name) {
            super(name, "io.debezium.connector.postgresql.PostgresConnector");
        }

        /**
         * IP address or hostname of the database server, has to be
         * specified.
         */
        public PostgresBuilder setDatabaseAddress(String address) {
            return setProperty("database.hostname", address);
        }

        /**
         * Optional port number of the database server, if unspecified
         * defaults to the database specific default port (5432).
         */
        public PostgresBuilder setDatabasePort(int port) {
            return setProperty("database.port", Integer.toString(port));
        }

        /**
         * Database user for connecting to the database server. Has to
         * be specified.
         */
        public PostgresBuilder setDatabaseUser(String user) {
            return setProperty("database.user", user);
        }

        /**
         * Database user password for connecting to the database server.
         * Has to be specified.
         */
        public PostgresBuilder setDatabasePassword(String password) {
            return setProperty("database.password", password);
        }

        /**
         * The name of the PostgreSQL database from which to stream the
         * changes. Has to be set.
         */
        public PostgresBuilder setDatabaseName(String dbName) {
            return setProperty("database.dbname", dbName);
        }

        /**
         * Logical name that identifies and provides a namespace for the
         * particular database server/cluster being monitored. The
         * logical name should be unique across all other connectors.
         * Only alphanumeric characters and underscores should be used.
         * Has to be specified.
         */
        public PostgresBuilder setClusterName(String cluster) {
            return setProperty("database.server.name", cluster);
        }

        /**
         * Optional regular expressions that match schema names to be
         * monitored ("schema" is used here to denote logical groups of
         * tables). Any schema name not included in the whitelist will
         * be excluded from monitoring. By default all non-system schemas
         * will be monitored. May not be used with
         * {@link #setSchemaBlacklist(String...) schema blacklist}.
         */
        public PostgresBuilder setSchemaWhitelist(String... schemaNameRegExps) {
            return setProperty("schema.whitelist", schemaNameRegExps);
        }

        /**
         * Optional regular expressions that match schema names to be
         * excluded from monitoring ("schema" is used here to denote
         * logical groups of tables). Any schema name not included in
         * the blacklist will be monitored, with the exception of system
         * schemas. May not be used with
         * {@link #setSchemaWhitelist(String...) schema whitelist}.
         */
        public PostgresBuilder setSchemaBlacklist(String... schemaNameRegExps) {
            return setProperty("schema.blacklist", schemaNameRegExps);
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
        public PostgresBuilder setTableWhitelist(String... tableNameRegExps) {
            return setProperty("table.whitelist", tableNameRegExps);
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any
         * table not included in the blacklist will be monitored. Each
         * identifier is of the form <i>schemaName.tableName</i>. May
         * not be used with
         * {@link #setTableWhitelist(String...) table whitelist}.
         */
        public PostgresBuilder setTableBlacklist(String... tableNameRegExps) {
            return setProperty("table.blacklist", tableNameRegExps);
        }

        /**
         * Optional regular expressions that match the fully-qualified
         * names of columns that should be excluded from change event
         * message values. Fully-qualified names for columns are of the
         * form <i>schemaName.tableName.columnName</i>.
         */
        public PostgresBuilder setColumnBlacklist(String... columnNameRegExps) {
            return setProperty("column.blacklist", columnNameRegExps);
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
            RULES.check(properties);
            return connect(properties,
                    CdcSources::createObjectMapper,
                    (mapper, event) -> event.timestamp(),
                    (mapper, record) -> {
                        String keyJson = Values.convertToString(record.keySchema(), record.key());
                        String valueJson = Values.convertToString(record.valueSchema(), record.value());
                        return new ChangeEventJsonImpl(keyJson, valueJson, mapper);
                    }
            );
        }
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a SQL Server database to Hazelcast Jet.
     */
    public static final class SqlServerBuilder extends AbstractBuilder<SqlServerBuilder> {

        private static final PropertyRules RULES = new PropertyRules()
                .mandatory("database.hostname")
                .mandatory("database.user")
                .mandatory("database.password")
                .mandatory("database.dbname")
                .mandatory("database.server.name")
                .exclusive("table.whitelist", "table.blacklist");

        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        private SqlServerBuilder(String name) {
            super(name, "io.debezium.connector.sqlserver.SqlServerConnector");
        }

        /**
         * IP address or hostname of the database server, has to be
         * specified.
         */
        public SqlServerBuilder setDatabaseAddress(String address) {
            return setProperty("database.hostname", address);
        }

        /**
         * Optional port number of the database server, if unspecified
         * defaults to the database specific default port (1433).
         */
        public SqlServerBuilder setDatabasePort(int port) {
            return setProperty("database.port", Integer.toString(port));
        }

        /**
         * Database user for connecting to the database server. Has to
         * be specified.
         */
        public SqlServerBuilder setDatabaseUser(String user) {
            return setProperty("database.user", user);
        }

        /**
         * Database user password for connecting to the database server.
         * Has to be specified.
         */
        public SqlServerBuilder setDatabasePassword(String password) {
            return setProperty("database.password", password);
        }

        /**
         * The name of the SQL Server database from which to stream the
         * changes. Has to be set.
         */
        public SqlServerBuilder setDatabaseName(String dbName) {
            return setProperty("database.dbname", dbName);
        }

        /**
         * Logical name that identifies and provides a namespace for the
         * particular database server/cluster being monitored. The
         * logical name should be unique across all other connectors.
         * Only alphanumeric characters and underscores should be used.
         * Has to be specified.
         */
        public SqlServerBuilder setClusterName(String cluster) {
            return setProperty("database.server.name", cluster);
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
        public SqlServerBuilder setTableWhitelist(String... tableNameRegExps) {
            return setProperty("table.whitelist", tableNameRegExps);
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any
         * table not included in the blacklist will be monitored. Each
         * identifier is of the form <i>schemaName.tableName</i>. May
         * not be used with
         * {@link #setTableWhitelist(String...) table whitelist}.
         */
        public SqlServerBuilder setTableBlacklist(String... tableNameRegExps) {
            return setProperty("table.blacklist", tableNameRegExps);
        }

        /**
         * Optional regular expressions that match the fully-qualified
         * names of columns that should be excluded from change event
         * message values. Fully-qualified names for columns are of the
         * form <i>schemaName.tableName.columnName</i>. Note that
         * primary key columns are always included in the event’s key,
         * even if blacklisted from the value.
         */
        public SqlServerBuilder setColumnBlacklist(String... columnNameRegExps) {
            return setProperty("column.blacklist", columnNameRegExps);
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
            RULES.check(properties);
            return connect(properties,
                    CdcSources::createObjectMapper,
                    (mapper, event) -> event.timestamp(),
                    (mapper, record) -> {
                        String keyJson = Values.convertToString(record.keySchema(), record.key());
                        String valueJson = Values.convertToString(record.valueSchema(), record.value());
                        return new ChangeEventJsonImpl(keyJson, valueJson, mapper);
                    }
            );
        }
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a MongoDB database to Hazelcast Jet.
     */
    public static final class MongoBuilder extends AbstractBuilder<MongoBuilder> {

        private static final PropertyRules RULES = new PropertyRules()
                .mandatory("mongodb.hosts")
                .mandatory("mongodb.name")
                .exclusive("database.whitelist", "database.blacklist")
                .exclusive("collection.whitelist", "collection.blacklist");

        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        private MongoBuilder(String name) {
            super(name, "io.debezium.connector.mongodb.MongoDbConnector");
        }

        /**
         * Hostname and port pairs (in the form 'host' or 'host:port')
         * of the MongoDB servers in the replica set. If
         * {@link #setMemberAutoDiscoveryEnabled(boolean) setMemberAutoDiscoveryEnabled()}
         * is set to {@code false}, then the host and port pair should
         * be prefixed with the replica set name (e.g.,
         * rs0/localhost:27017). Has to be specified.
         */
        public MongoBuilder setDatabaseHosts(String... hosts) {
            return setProperty("mongodb.hosts", hosts);
        }

        /**
         * Unique name that identifies the MongoDB replica set or
         * sharded cluster that this connector monitors. Only
         * alphanumeric characters and underscores should be used. Has
         * to be specified.
         */
        public MongoBuilder setClusterName(String cluster) {
            return setProperty("mongodb.name", cluster);
        }

        /**
         * Name of the database user to be used when connecting to
         * MongoDB. This is required only when MongoDB is configured to
         * use authentication.
         */
        public MongoBuilder setDatabaseUser(String user) {
            return setProperty("mongodb.user", user);
        }

        /**
         * Password to be used when connecting to MongoDB. This is
         * required only when MongoDB is configured to use
         * authentication.
         */
        public MongoBuilder setDatabasePassword(String password) {
            return setProperty("mongodb.password", password);
        }

        /**
         * Database (authentication source) containing MongoDB
         * credentials. This is required only when MongoDB is
         * configured to use authentication with another authentication
         * database than admin. Defaults to 'admin'.
         */
        public MongoBuilder setAuthSource(String authSource) {
            return setProperty("mongodb.authsource", authSource);
        }

        /**
         * Sets if connector will use SSL to connect to MongoDB
         * instances. Defaults to {@code false}.
         */
        public MongoBuilder setSslEnabled(boolean enabled) {
            return setProperty("mongodb.ssl.enabled", enabled);
        }

        /**
         * When SSL is enabled this setting controls whether strict
         * hostname checking is disabled during connection phase. If
         * {@code true} the connection will not prevent
         * man-in-the-middle attacks. Defaults to {@code false}.
         */
        public MongoBuilder setSslInvalidHostnameAllowed(boolean allowed) {
            return setProperty("mongodb.ssl.invalid.hostname.allowed", allowed);
        }

        /**
         * Optional regular expressions that match database names to be
         * monitored; any database name not included in the whitelist
         * will be excluded from monitoring. By default all databases
         * will be monitored. May not be used with
         * {@link #setDatabaseBlacklist(String...) database blacklist}.
         */
        public MongoBuilder setDatabaseWhitelist(String... dbNameRegExps) {
            return setProperty("database.whitelist", dbNameRegExps);
        }

        /**
         * Optional regular expressions that match database names to be
         * excluded from monitoring; any database name not included in
         * the blacklist will be monitored. May not be used with
         * {@link #setDatabaseWhitelist(String...) database whitelist}.
         */
        public MongoBuilder setDatabaseBlacklist(String... dbNameRegExps) {
            return setProperty("database.blacklist", dbNameRegExps);
        }

        /**
         * Optional regular expressions that match fully-qualified
         * namespaces for MongoDB collections to be monitored; any
         * collection name not included in the whitelist will be
         * excluded from monitoring. Each identifier is of the form
         * <i>databaseName.collectionName</i>. By default all collections
         * will be monitored, except those in the 'local' and 'admin'
         * databases. May not be used with
         * {@link #setCollectionBlacklist(String...) collection blacklist}.
         */
        public MongoBuilder setCollectionWhitelist(String... collectionNameRegExps) {
            return setProperty("collection.whitelist", collectionNameRegExps);
        }

        /**
         * Optional regular expressions that match fully-qualified
         * namespaces for MongoDB collections to be excluded from
         * monitoring; any collection name not included in the blacklist
         * will be monitored. Each identifier is of the form
         * <i>databaseName.collectionName</i> May not be used with
         * {@link #setCollectionWhitelist(String...) collection whitelist}.
         */
        public MongoBuilder setCollectionBlacklist(String... collectionNameRegExps) {
            return setProperty("collection.blacklist", collectionNameRegExps);
        }

        /**
         * Optional expressions that match fully-qualified names
         * of fields that should be excluded from change event message
         * values. Fully-qualified names for fields are of the form
         * <i>databaseName.collectionName.fieldName.nestedFieldName</i>,
         * where <i>databaseName</i> and <i>collectionName</i> may
         * contain the wildcard ({@code *}) which matches any characters.
         */
        public MongoBuilder setFieldBlacklist(String... fieldNameRegExps) {
            return setProperty("field.blacklist", fieldNameRegExps);
        }

        /**
         * Specifies whether the addresses in {@link #setDatabaseHosts(String...) setDatabaseHosts()}
         * are seeds that should be used to discover all members of the
         * cluster or replica set ({@code true}), or whether they should
         * be used as is ({@code false}). The default is {@code true}
         * and should be used in all cases except where MongoDB is
         * fronted by a proxy.
         */
        public MongoBuilder setMemberAutoDiscoveryEnabled(boolean enabled) {
            return setProperty("mongodb.members.auto.discover", enabled);
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
            RULES.check(properties);
            return connect(properties,
                    () -> null,
                    (IGNORED, event) -> event.timestamp(),
                    (IGNORED, record) -> {
                        String keyJson = Values.convertToString(record.keySchema(), record.key());
                        String valueJson = Values.convertToString(record.valueSchema(), record.value());
                        return new ChangeEventMongoImpl(keyJson, valueJson);
                    }
            );
        }
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from any Debezium supported database to Hazelcast Jet.
     */
    public static final class DebeziumBuilder extends AbstractBuilder<DebeziumBuilder> {

        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        private DebeziumBuilder(String name, String connectorClass) {
            super(name, connectorClass);
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
            return connect(properties,
                    CdcSources::createObjectMapper,
                    (mapper, event) -> event.value().getLong("ts_ms").orElse(0L),
                    (mapper, record) -> {
                        String keyJson = Values.convertToString(record.keySchema(), record.key());
                        String valueJson = Values.convertToString(record.valueSchema(), record.value());
                        return new ChangeEventJsonImpl(keyJson, valueJson, mapper);
                    }
            );
        }
    }

    private static class KafkaConnectSource extends AbstractKafkaConnectSource<ChangeEvent> {

        private final ObjectMapper objectMapper;
        private final BiFunctionEx<ObjectMapper, SourceRecord, ChangeEvent> recordToEventMapper;
        private final BiFunctionEx<ObjectMapper, ChangeEvent, Long> timestampProjectionFn;

        KafkaConnectSource(
                Processor.Context ctx,
                Properties properties,
                SupplierEx<ObjectMapper> objectMapperSupplier,
                BiFunctionEx<ObjectMapper, SourceRecord, ChangeEvent> recordToEventMapper,
                BiFunctionEx<ObjectMapper, ChangeEvent, Long> eventToTimestampMapper
        ) {
            super(injectHazelcastInstanceNameProperty(ctx, properties));
            this.objectMapper = objectMapperSupplier.get();
            this.recordToEventMapper = recordToEventMapper;
            this.timestampProjectionFn = eventToTimestampMapper;
        }

        @Override
        protected boolean addToBuffer(SourceRecord record, SourceBuilder.TimestampedSourceBuffer<ChangeEvent> buf) {
            ChangeEvent event = recordToEventMapper.apply(objectMapper, record);
            if (event != null) {
                long ts = timestampProjectionFn.apply(objectMapper, event);
                buf.add(event, ts);
                return true;
            }
            return false;
        }

        private static Properties injectHazelcastInstanceNameProperty(Processor.Context ctx, Properties properties) {
            JetInstance jet = ctx.jetInstance();
            String instanceName = HazelcastInstanceFactory.getInstanceName(jet.getName(),
                    jet.getHazelcastInstance().getConfig());
            properties.setProperty("database.history.hazelcast.instance.name", instanceName);
            //needed only by CDC sources... could be achieved via one more lambda, but we do have enough of those...
            return properties;
        }
    }

}
