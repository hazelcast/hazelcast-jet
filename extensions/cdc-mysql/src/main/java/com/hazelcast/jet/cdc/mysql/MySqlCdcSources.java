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

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.impl.CdcSource;
import com.hazelcast.jet.cdc.impl.ChangeRecordCdcSource;
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.cdc.mysql.impl.MySqlSequenceExtractor;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Properties;

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
     * Creates a CDC source that streams change data from a MySQL database to
     * Hazelcast Jet.
     * <p>
     * Behaviour of the source on connection disruptions to the database is
     * configurable and is governed by the {@code setReconnectBehaviour(String)}
     * setting (as far as the underlying Debezium connector cooperates, read
     * further for details).
     * <p>
     * The default reconnect behaviour is <em>FAIL</em>, which threats any
     * connection failure as an unrecoverable problem and produces the failure
     * of the source and the entire job. (How Jet handles job failures and what
     * ways there are for recovering from them, is a generic issue not discussed
     * here.)
     * <p>
     * The other two behaviour options, <em>RECONNECT</em> and
     * <em>CLEAR_STATE_AND_RECONNECT</em>, instruct the source to try to
     * automatically recover from any connection failure by reconnecting,
     * either via the connector's internal reconnect mechanisms or by restarting
     * the whole source. The two types of behaviour differ from each-other in
     * how exactly they handle the source restart, if they preserve the current
     * state of the source or if they reset it. If the state is kept, then
     * snapshotting should not be repeated and streaming the binlog should
     * resume at the position where it left off. If the state is reset, then the
     * source will behave as if it were its initial start, so will do a snapshot
     * and will start trailing the binlog where it syncs with the snapshot's
     * end.
     * <p>
     * Depending on the lifecycle phase the source is in, however, there are
     * some discrepancies and peculiarities in this behaviour. There are also
     * further settings for influencing it. See what follows for details.
     * <p>
     * On the <em>initial start</em> of the connector, if the reconnect
     * behaviour is set to <em>FAIL</em> and the database is not immediately
     * reachable, the source will fail. Otherwise, it will try to reconnect
     * until it succeeds. How much it will wait between two successive reconnect
     * attempts can be configured via the {@code setReconnectIntervalMs(long)}
     * setting.
     * <p>
     * If the connection to the database fails <em>during the snapshotting
     * phase</em> then the connector is stuck in this state until it manages to
     * reconnect. This, unfortunately, is the case even when reconnect behaviour
     * is set to <em>FAIL</em> and is related to the peculiarities of the
     * underlying Debezium connector's implementation. If the connection goes
     * down due to the database being shut down, it sometimes can detect that
     * and react properly, but if the outage is purely at the network level,
     * then, more often than not, it's not detected.
     * <p>
     * During the <em>binlog trailing</em> phase all connection disruptions
     * will be detected, but internally not all of them are handled the same
     * way. If the database is shut down, then the connector can detect that
     * and will not handle it. It will just fail and, depending on the reconnect
     * behaviour, Jet can trigger the restarting of the source. If the outage
     * is at the network level or a database shutdown is not detected as such,
     * then the connector will trigger internal reconnecting, even if the
     * source's reconnect behaviour is set to <em>FAIL</em> and will do this
     * until it manages to connect. The frequency of these attempt can also
     * be influenced via the {@code setReconnectIntervalMs(long)} setting.
     *
     * @param name name of this source, needs to be unique, will be passed to
     *             the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also to
     * construct the source once configuration is done
     */
    @Nonnull
    public static Builder mysql(@Nonnull String name) {
        return new Builder(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a MySQL database to Hazelcast Jet.
     */
    public static final class Builder {

        private static final PropertyRules RULES = new PropertyRules()
                .required("database.hostname")
                .required("database.user")
                .required("database.password")
                .required("database.server.name")
                .exclusive("database.whitelist", "database.blacklist")
                .exclusive("table.whitelist", "table.blacklist");

        private final DebeziumConfig config;

        /**
         * @param name name of the source, needs to be unique,
         *             will be passed to the underlying Kafka
         *             Connect source
         */
        private Builder(String name) {
            Objects.requireNonNull(name, "name");

            config = new DebeziumConfig(name, "io.debezium.connector.mysql.MySqlConnector");
            config.setProperty(CdcSource.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, MySqlSequenceExtractor.class.getName());
            config.setProperty("include.schema.changes", "false");

            config.setProperty("connect.keep.alive", "true");
            config.setProperty("connect.keep.alive.interval.ms", CdcSource.DEFAULT_RECONNECT_INTERVAL_MS);
        }

        /**
         * IP address or hostname of the database server, has to be specified.
         */
        @Nonnull
        public Builder setDatabaseAddress(@Nonnull String address) {
            config.setProperty("database.hostname", address);
            return this;
        }

        /**
         * Optional port number of the database server, if unspecified defaults
         * to the database specific default port (3306).
         */
        @Nonnull
        public Builder setDatabasePort(int port) {
            config.setProperty("database.port", Integer.toString(port));
            return this;
        }

        /**
         * Database user for connecting to the database server. Has to be
         * specified.
         */
        @Nonnull
        public Builder setDatabaseUser(@Nonnull String user) {
            config.setProperty("database.user", user);
            return this;
        }

        /**
         * Database user password for connecting to the database server. Has to
         * be specified.
         */
        @Nonnull
        public Builder setDatabasePassword(@Nonnull String password) {
            config.setProperty("database.password", password);
            return this;
        }

        /**
         * Logical name that identifies and provides a namespace for the
         * particular database server/cluster being monitored. The logical name
         * should be unique across all other connectors. Only alphanumeric
         * characters and underscores should be used. Has to be specified.
         */
        @Nonnull
        public Builder setClusterName(@Nonnull String cluster) {
            config.setProperty("database.server.name", cluster);
            return this;
        }

        /**
         * A numeric ID of this database client, which must be unique across all
         * currently-running database processes in the MySQL cluster. This
         * connector joins the MySQL database cluster as another server (with
         * this unique ID) so it can read the binlog. By default, a random
         * number is generated between 5400 and 6400, though we recommend
         * setting an explicit value.
         */
        @Nonnull
        public Builder setDatabaseClientId(int clientId) {
            config.setProperty("database.server.id", clientId);
            return this;
        }

        /**
         * Optional regular expressions that match database names to be
         * monitored; any database name not included in the whitelist will be
         * excluded from monitoring. By default all databases will be monitored.
         * May not be used with {@link #setDatabaseBlacklist(String...) database
         * blacklist}.
         */
        @Nonnull
        public Builder setDatabaseWhitelist(@Nonnull String... dbNameRegExps) {
            config.setProperty("database.whitelist", dbNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match database names to be excluded
         * from monitoring; any database name not included in the blacklist will
         * be monitored. May not be used with
         * {@link #setDatabaseWhitelist(String...) database whitelist}.
         */
        @Nonnull
        public Builder setDatabaseBlacklist(@Nonnull String... dbNameRegExps) {
            config.setProperty("database.blacklist", dbNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be monitored; any table not included in the
         * whitelist will be excluded from monitoring. Each identifier is of the
         * form <em>databaseName.tableName</em>. By default the connector will
         * monitor every non-system table in each monitored database. May not be
         * used with {@link #setTableBlacklist(String...) table blacklist}.
         */
        @Nonnull
        public Builder setTableWhitelist(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.whitelist", tableNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any table not
         * included in the blacklist will be monitored. Each identifier is of
         * the form <em>databaseName.tableName</em>. May not be used with
         * {@link #setTableWhitelist(String...) table whitelist}.
         */
        @Nonnull
        public Builder setTableBlacklist(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.blacklist", tableNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match the fully-qualified names of
         * columns that should be excluded from change event message values.
         * Fully-qualified names for columns are of the form
         * <em>databaseName.tableName.columnName</em>, or
         * <em>databaseName.schemaName.tableName.columnName</em>.
         */
        @Nonnull
        public Builder setColumnBlacklist(@Nonnull String... columnNameRegExps) {
            config.setProperty("column.blacklist", columnNameRegExps);
            return this;
        }

        /**
         * Specifies whether to use an encrypted connection to the database. The
         * default is <em>disabled</em>, and specifies to use an unencrypted
         * connection.
         * <p>
         * The <em>preferred</em> option establishes an encrypted connection if
         * the server supports secure connections but falls back to an
         * unencrypted connection otherwise.
         * <p>
         * The <em>required</em> option establishes an encrypted connection but
         * will fail if one cannot be made for any reason.
         * <p>
         * The <em>verify_ca</em> option behaves like <em>required</em> but
         * additionally it verifies the server TLS certificate against the
         * configured Certificate Authority (CA) certificates and will fail if
         * it doesn’t match any valid CA certificates.
         * <p>
         * The <em>verify_identity</em> option behaves like <em>verify_ca</em> but
         * additionally verifies that the server certificate matches the host of
         * the remote connection.
         */
        @Nonnull
        public Builder setSslMode(@Nonnull String mode) {
            config.setProperty("database.ssl.mode", mode);
            return this;
        }

        /**
         * Specifies the (path to the) Java keystore file containing the
         * database client certificate and private key.
         * <p>
         * Can be alternatively specified via the 'javax.net.ssl.keyStore'
         * system or JVM property.
         */
        @Nonnull
        public Builder setSslKeystoreFile(@Nonnull String file) {
            config.setProperty("database.ssl.keystore", file);
            return this;
        }

        /**
         * Password to access the private key from any specified keystore files.
         * <p>
         * This password is used to unlock the keystore file (store password),
         * and to decrypt the private key stored in the keystore (key password)."
         * <p>
         * Can be alternatively specified via the 'javax.net.ssl.keyStorePassword'
         * system or JVM property.
         */
        @Nonnull
        public Builder setSslKeystorePassword(@Nonnull String password) {
            config.setProperty("database.ssl.keystore.password", password);
            return this;
        }

        /**
         * Specifies the (path to the) Java truststore file containing the
         * collection of trusted CA certificates.
         * <p>
         * Can be alternatively specified via the 'javax.net.ssl.trustStore'
         * system or JVM property.
         */
        @Nonnull
        public Builder setSslTruststoreFile(@Nonnull String file) {
            config.setProperty("database.ssl.truststore", file);
            return this;
        }

        /**
         * Password to unlock any specified truststore.
         * <p>
         * Can be alternatively specified via the 'javax.net.ssl.trustStorePassword'
         * system or JVM property.
         */
        @Nonnull
        public Builder setSslTruststorePassword(@Nonnull String password) {
            config.setProperty("database.ssl.truststore.password", password);
            return this;
        }

        /**
         * Interval in milliseconds after which to do periodic connection checking
         * and initiate reconnect, if necessary. Defaults to
         * {@value CdcSource#DEFAULT_RECONNECT_INTERVAL_MS} milliseconds.
         */
        @Nonnull
        public Builder setReconnectIntervalMs(long intervalMs) {
            config.setProperty(CdcSource.RECONNECT_INTERVAL_MS, intervalMs);
            config.setProperty("connect.keep.alive.interval.ms", intervalMs);
            return this;
        }

        /**
         * Specifies how the connector should behave when it detects that the
         * backing database has been shut down (note: temporary connection
         * disruptions will not be interpreted in this way; after simple
         * network outages the connector will automatically reconnect,
         * regardless of this setting).
         * <p>
         * Possible values are (they are <em>not</em> case sensitive):
         * <ul>
         *     <li><em>FAIL</em>: will cause the whole job to fail</li>
         *     <li><em>CLEAR_STATE_AND_RECONNECT</em>: will reconnect to
         *      database, but will clear all internal state first, thus behaving
         *      as if it would be connecting the first time (for example
         *      snapshotting will be repeated)</li>
         *     <li><em>RECONNECT</em>: will reconnect as is, in the same state
         *      as it was at the moment of the disconnect </li>
         * </ul>
         */
        @Nonnull
        public Builder setReconnectBehaviour(String behaviour) {
            config.setProperty(CdcSource.RECONNECT_BEHAVIOUR_PROPERTY, behaviour);
            return this;
        }

        /**
         * Can be used to set any property not explicitly covered by other
         * methods or to override properties we have hidden.
         */
        @Nonnull
        public Builder setCustomProperty(@Nonnull String key, @Nonnull String value) {
            config.setProperty(key, value);
            return this;
        }

        /**
         * Returns the source based on the properties set so far.
         */
        @Nonnull
        public StreamSource<ChangeRecord> build() {
            Properties properties = config.toProperties();
            RULES.check(properties);
            return ChangeRecordCdcSource.fromProperties(properties);
        }

    }
}
