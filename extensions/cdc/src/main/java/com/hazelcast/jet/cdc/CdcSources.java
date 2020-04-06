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

    //todo: use BUILDER instead of Properties

    //todo: can we use these sources in a distributed way?

    //todo: update main README.md file in cdc module

    //todo: review all Debezium config options, see if we need to add more

    //todo: further refine and document config option

    //todo: the ObjectMapper, even if it's serializable, shouldn't be in each event!

    //todo: sources could have an optional, custom ObjectMapper param, which would simplify the definition of data objects

    private CdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from your MySQL
     * database to the Hazelcast Jet pipeline.
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
     * Creates a CDC source that streams change data from your MySQL
     * database to the Hazelcast Jet pipeline.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @param properties configuration object which holds the configuration
     *                   properties of the connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<ChangeEvent> mysql(String name, Properties properties) { //todo: replace with builder
        properties = copy(properties);

        properties.put("name", name);
        properties.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");

        properties.putIfAbsent("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.putIfAbsent("database.history.hazelcast.list.name", name);

        /*Flag that specifies if the connector should generate on the
        schema change topic named 'fulfillment' events with DDL changes
        that can be used by consumers.*/
        properties.putIfAbsent("include.schema.changes", "false");

        properties.putIfAbsent("tombstones.on.delete", "false"); //todo: can this be parsed, if enabled? force it?

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

    /**
     * Creates a CDC source that streams change data from your
     * PostgreSQL database to the Hazelcast Jet pipeline.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @param properties configuration object which holds the configuration
     *                   properties of the connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<ChangeEvent> postgres(String name, Properties properties) { //todo: replace with builder
        properties = copy(properties);

        properties.put("name", name);
        properties.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        properties.putIfAbsent("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.putIfAbsent("database.history.hazelcast.list.name", name);

        properties.putIfAbsent("tombstones.on.delete", "false");

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

    /**
     * Creates a CDC source that streams change data from your
     * Microsoft SQL Server database to the Hazelcast Jet pipeline.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @param properties configuration object which holds the configuration
     *                   properties of the connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<ChangeEvent> sqlserver(String name, Properties properties) { //todo: replace with builder
        properties = copy(properties);

        properties.put("name", name);
        properties.put("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");

        properties.putIfAbsent("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.putIfAbsent("database.history.hazelcast.list.name", name);

        properties.putIfAbsent("tombstones.on.delete", "false");

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

    /**
     * Creates a CDC source that streams change data from your
     * MongoDB database to the Hazelcast Jet pipeline.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @param properties configuration object which holds the configuration
     *                   properties of the connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<ChangeEvent> mongodb(String name, Properties properties) { //todo: replace with builder
        properties = copy(properties);

        /* Used internally as a unique identifier when recording the
        oplog position of each replica set. Needs to be set. */
        checkSet(properties, "mongodb.name");

        properties.put("name", name);
        properties.put("connector.class", "io.debezium.connector.mongodb.MongoDbConnector");

        properties.putIfAbsent("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.putIfAbsent("database.history.hazelcast.list.name", name);

        /* When running the connector against a sharded cluster, use a
        value of tasks.max that is greater than the number of replica
        sets. This will allow the connector to create one task for each
        replica set, and will let Kafka Connect coordinate, distribute,
        and manage the tasks across all of the available worker
        processes.*/
        properties.putIfAbsent("tasks.max", 1);

        /*Positive integer value that specifies the maximum number of
        threads used to perform an intial sync of the collections in a
        replica set.*/
        properties.putIfAbsent("initial.sync.max.threads", 1);

        properties.putIfAbsent("tombstones.on.delete", "false");

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

    /**
     * Creates a CDC source that streams change data from your Debezium
     * supported database to the Hazelcast Jet pipeline.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @param properties configuration object which holds the configuration
     *                   properties of the connector.
     * @return a source to use in {@link com.hazelcast.jet.pipeline.Pipeline#readFrom(StreamSource)}
     */
    public static StreamSource<ChangeEvent> debezium(String name, Properties properties) {
        properties = copy(properties);

        properties.put("name", name);
        checkSet(properties, "connector.class");

        properties.putIfAbsent("database.history", HazelcastListDatabaseHistory.class.getName());
        properties.putIfAbsent("database.history.hazelcast.list.name", name);

        properties.putIfAbsent("tombstones.on.delete", "false");

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

    private static void checkSet(Properties properties, String key) {
        if (properties.get(key) == null) {
            throw new IllegalArgumentException("'" + key + "' should be set");
        }
    }

    private static Properties copy(Properties properties) {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
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
    }

    /**
     * TODO: javadoc
     */
    public static final class MySqlBuilder extends AbstractBuilder<MySqlBuilder> {

        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        public MySqlBuilder(String name) {
            super(name, "io.debezium.connector.mysql.MySqlConnector");
            properties.put("include.schema.changes", "false");
        }

        /**
         * IP address or hostname of the MySQL database server.
         */
        public MySqlBuilder setDatabaseAddress(String address) {
            properties.put("database.hostname", address); //todo: no default, check is set on build
            return this;
        }

        /**
         * Integer port number of the MySQL database server, defaults to
         * 3306.
         */
        public MySqlBuilder setDatabasePort(Integer port) {
            properties.put("database.port", port);
            return this;
        }

        /**
         * Database user for connecting to the MySQL database server.
         */
        public MySqlBuilder setDatabaseUser(String user) {
            properties.put("database.user", user); //todo: no default, check is set on build
            return this;
        }

        /**
         * Database user password for connecting to the MySQL database
         * server.
         */
        public MySqlBuilder setDatabasePassword(String password) {
            properties.put("database.password", password); //todo: mandatory, check on build
            return this;
        }

        /**
         * Logical name that identifies and provides a namespace for the
         * particular MySQL database server/cluster being monitored. The
         * logical name should be unique across all other connectors.
         * Only alphanumeric characters and underscores should be used.
         */
        public MySqlBuilder setDatabaseClusterName(String cluster) {
            properties.put("database.server.name", cluster); //todo: mandatory? needed? check on build
            return this;
        }

        /**
         * A numeric ID of this database client, which must be unique
         * across all currently-running database processes in the MySQL
         * cluster. This connector joins the MySQL database cluster as
         * another server (with this unique ID) so it can read the
         * binlog. By default, a random number is generated between
         * 5400 and 6400, though we recommend setting an explicit value.
         */
        public MySqlBuilder setDatabaseClientId(int id) {
            properties.put("database.server.id", id);
            return this;
        }

        /**
         * An optional comma-separated list of regular expressions that
         * match database names to be monitored; any database name not
         * included in the whitelist will be excluded from monitoring.
         * By default all databases will be monitored. May not be used
         * with database whitelist. //todo: check this on build
         */
        public MySqlBuilder setDatabaseWhitelist(String whitelist) { //todo: use String[] instead?
            properties.put("database.whitelist", whitelist);
            return this;
        }

        /**
         * An optional comma-separated list of regular expressions that
         * match fully-qualified table identifiers for tables to be
         * monitored; any table not included in the whitelist will be
         * excluded from monitoring. Each identifier is of the form
         * 'databaseName.tableName'. By default the connector will
         * monitor every non-system table in each monitored database.
         * May not be used with table blacklist.
         */
        public MySqlBuilder setTableWhitelist(String whitelist) {
            properties.put("table.whitelist", whitelist);
            return this;
        }

        //todo: cover all other properties

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
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
