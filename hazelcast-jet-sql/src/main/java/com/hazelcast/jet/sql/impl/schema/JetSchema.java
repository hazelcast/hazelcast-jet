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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Schema operating on registered sources/sinks
 */
public class JetSchema extends AbstractSchema {

    /**
     * The name under which the IMap connector is registered. Use as an
     * argument to {@link #createServer} when connecting to remote cluster.
     */
    public static final String IMAP_CONNECTOR = "imap";

    /**
     * The server name under which the local hazelcast IMap connector registered.
     */
    public static final String IMAP_LOCAL_SERVER = "local_imap";

    public static final String OPTION_CLASS_NAME = "className";
    private static final String OPTION_CONNECTOR_NAME = JetSchema.class + ".connectorName";
    private static final String OPTION_SERVER_NAME = JetSchema.class + ".serverName";

    private final ConcurrentMap<String, SqlConnector> connectorMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Map<String, String>> serverMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, JetTable> tableMap = new ConcurrentHashMap<>();

    private final Map<String, Table> unmodifiableTableMap = Collections.unmodifiableMap(tableMap);
    private final JetInstance instance;

    public JetSchema(JetInstance instance) {
        this.instance = instance;

        // insert the IMap connector and local cluster server by default
        createConnector(IMAP_CONNECTOR, new IMapSqlConnector());
        createServer(IMAP_LOCAL_SERVER, IMAP_CONNECTOR, emptyMap());
    }

    @Override
    public Map<String, Table> getTableMap() {
        return unmodifiableTableMap;
    }

    public void createConnector(String connectorName, Map<String, String> connectorOptions) {
        String className = requireNonNull(connectorOptions.get(OPTION_CLASS_NAME), "missing " + OPTION_CLASS_NAME + " option");
        SqlConnector connector = newInstance(className);
        createConnector(connectorName, connector);
    }

    private void createConnector(String connectorName, SqlConnector connector) {
        connectorMap.put(connectorName, connector);
    }

    public void createServer(String serverName, String connectorName, Map<String, String> serverOptions) {
        serverOptions = new HashMap<>(serverOptions); // convert to a HashMap so that we can mutate it
        if (!connectorMap.containsKey(connectorName)) {
            throw new IllegalArgumentException("Unknown connector: " + connectorName);
        }
        if (serverOptions.put(OPTION_CONNECTOR_NAME, connectorName) != null) {
            throw new IllegalArgumentException("Private option used");
        }
        serverMap.put(serverName, serverOptions);
    }

    public void createTable(
            @Nonnull String tableName,
            @Nonnull String serverName,
            @Nonnull Map<String, String> tableOptions,
            @Nullable List<Entry<String, QueryDataType>> fields
    ) {
        tableOptions = new HashMap<>(tableOptions); // convert to a HashMap so that we can mutate it
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

        JetTable table;
        if (fields == null) {
            table = connector.createTable(instance, tableName, serverOptions, tableOptions);
        } else {
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("zero fields");
            }
            if (fields.stream().anyMatch(e -> e.getValue() == QueryDataType.NULL)) {
                throw new IllegalArgumentException("NULL type not supported");
            }
            table = connector.createTable(instance, tableName, serverOptions, tableOptions, fields);
        }
        tableMap.put(tableName, table);
    }
}
