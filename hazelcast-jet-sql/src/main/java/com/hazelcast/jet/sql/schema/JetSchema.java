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

package com.hazelcast.jet.sql.schema;

import com.hazelcast.jet.sql.SqlConnector;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Schema operating on registered sources/sinks
 */
public class JetSchema extends AbstractSchema {

    /**
     * The name under which the IMap connector is registered. Use as an
     * argument to {@link #putServer} when connecting to remote cluster.
     */
    public static final String IMAP_CONNECTOR_NAME = "imap";

    /** The server name under which the local hazelcast cluster is registered. */
    public static final String LOCAL_HAZELCAST_CLUSTER = "local_hz";

    private static final String OPTION_CONNECTOR_NAME = JetSchema.class + ".connectorName";
    private static final String OPTION_SERVER_NAME = JetSchema.class + ".serverName";

    private final ConcurrentMap<String, SqlConnector> connectorMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Map<String, String>> serverMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, JetTable> tableMap = new ConcurrentHashMap<>();

    private final Map<String, Table> unmodifiableTableMap = Collections.unmodifiableMap(tableMap);

    public JetSchema() {
    }

    @Override
    public Map<String, Table> getTableMap() {
        return unmodifiableTableMap;
    }

    public void putConnector(String connectorName, SqlConnector connector) {
        connectorMap.put(connectorName, connector);
    }

    public void putServer(String serverName, String connectorName, Map<String, String> options) {
        if (!connectorMap.containsKey(connectorName)) {
            throw new IllegalArgumentException("Unknown connector: " + connectorName);
        }
        if (options.put(OPTION_CONNECTOR_NAME, connectorName) != null) {
            throw new IllegalArgumentException("Private option used");
        }
        serverMap.put(serverName, options);
    }

    public void putTable(
            @Nonnull String tableName,
            @Nonnull String serverName,
            @Nonnull Map<String, String> tableOptions
    ) {
        Map<String, String> serverOptions = serverMap.get(serverName);
        if (serverOptions == null) {
            throw new IllegalArgumentException("Unknown server: " + serverName);
        }
        String connectorName = serverOptions.get(OPTION_CONNECTOR_NAME);
        SqlConnector connector = connectorMap.get(connectorName);
        if (connector == null) {
            throw new IllegalArgumentException("Server references unknown connector: " + connectorName);
        }
        if (tableOptions.put(OPTION_SERVER_NAME, serverName) != null) {
            throw new IllegalArgumentException("Private option used");
        }

        JetTable table = connector.createTable(tableName, serverOptions, tableOptions);
        tableMap.put(serverName, table);
    }
}
