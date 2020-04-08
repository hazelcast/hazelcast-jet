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

package com.hazelcast.jet.cdc.mongodb;

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.cdc.impl.AbstractSourceBuilder;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.connect.data.Values;

/**
 * Contains factory methods for creating change data capture sources
 *
 * @since 4.1
 */
@EvolvingApi
public final class MongoDbCdcSources {

    private MongoDbCdcSources() {
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
    public static Builder mongodb(String name) {
        return new Builder(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a MongoDB database to Hazelcast Jet.
     */
    public static final class Builder extends AbstractSourceBuilder<Builder> {

        private static final PropertyRules RULES = new PropertyRules()
                .mandatory("mongodb.hosts")
                .mandatory("mongodb.name")
                .exclusive("database.whitelist", "database.blacklist")
                .exclusive("collection.whitelist", "collection.blacklist");

        /**
         * Name of the source, needs to be unique, will be passed to the
         * underlying Kafka Connect source.
         */
        private Builder(String name) {
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
        public Builder setDatabaseHosts(String... hosts) {
            return setProperty("mongodb.hosts", hosts);
        }

        /**
         * Unique name that identifies the MongoDB replica set or
         * sharded cluster that this connector monitors. Only
         * alphanumeric characters and underscores should be used. Has
         * to be specified.
         */
        public Builder setClusterName(String cluster) {
            return setProperty("mongodb.name", cluster);
        }

        /**
         * Name of the database user to be used when connecting to
         * MongoDB. This is required only when MongoDB is configured to
         * use authentication.
         */
        public Builder setDatabaseUser(String user) {
            return setProperty("mongodb.user", user);
        }

        /**
         * Password to be used when connecting to MongoDB. This is
         * required only when MongoDB is configured to use
         * authentication.
         */
        public Builder setDatabasePassword(String password) {
            return setProperty("mongodb.password", password);
        }

        /**
         * Database (authentication source) containing MongoDB
         * credentials. This is required only when MongoDB is
         * configured to use authentication with another authentication
         * database than admin. Defaults to 'admin'.
         */
        public Builder setAuthSource(String authSource) {
            return setProperty("mongodb.authsource", authSource);
        }

        /**
         * Sets if connector will use SSL to connect to MongoDB
         * instances. Defaults to {@code false}.
         */
        public Builder setSslEnabled(boolean enabled) {
            return setProperty("mongodb.ssl.enabled", enabled);
        }

        /**
         * When SSL is enabled this setting controls whether strict
         * hostname checking is disabled during connection phase. If
         * {@code true} the connection will not prevent
         * man-in-the-middle attacks. Defaults to {@code false}.
         */
        public Builder setSslInvalidHostnameAllowed(boolean allowed) {
            return setProperty("mongodb.ssl.invalid.hostname.allowed", allowed);
        }

        /**
         * Optional regular expressions that match database names to be
         * monitored; any database name not included in the whitelist
         * will be excluded from monitoring. By default all databases
         * will be monitored. May not be used with
         * {@link #setDatabaseBlacklist(String...) database blacklist}.
         */
        public Builder setDatabaseWhitelist(String... dbNameRegExps) {
            return setProperty("database.whitelist", dbNameRegExps);
        }

        /**
         * Optional regular expressions that match database names to be
         * excluded from monitoring; any database name not included in
         * the blacklist will be monitored. May not be used with
         * {@link #setDatabaseWhitelist(String...) database whitelist}.
         */
        public Builder setDatabaseBlacklist(String... dbNameRegExps) {
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
        public Builder setCollectionWhitelist(String... collectionNameRegExps) {
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
        public Builder setCollectionBlacklist(String... collectionNameRegExps) {
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
        public Builder setFieldBlacklist(String... fieldNameRegExps) {
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
        public Builder setMemberAutoDiscoveryEnabled(boolean enabled) {
            return setProperty("mongodb.members.auto.discover", enabled);
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        public StreamSource<ChangeEvent> build() {
            RULES.check(properties);
            return connect(properties,
                    ChangeEvent::timestamp,
                    (record) -> {
                        String keyJson = Values.convertToString(record.keySchema(), record.key());
                        String valueJson = Values.convertToString(record.valueSchema(), record.value());
                        return new ChangeEventMongoImpl(keyJson, valueJson);
                    }
            );
        }
    }
}
