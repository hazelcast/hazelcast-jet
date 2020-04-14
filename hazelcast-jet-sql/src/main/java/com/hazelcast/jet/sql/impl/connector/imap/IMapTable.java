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

package com.hazelcast.jet.sql.impl.connector.imap;

import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlWriters.EntryWriter;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.schema.Table;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.Util.toList;

/**
 * {@link Table} implementation for IMap.
 */
public class IMapTable extends JetTable {

    private final String mapName;

    private final EntryWriter writer;

    public IMapTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull String mapName,
            @Nonnull List<Entry<String, QueryDataType>> fields,
            @Nonnull EntryWriter writer
    ) {
        super(sqlConnector, fields);
        this.mapName = mapName;

        this.writer = writer;
    }

    String getMapName() {
        return mapName;
    }

    EntryWriter getWriter() {
        return writer;
    }

    @Override
    public boolean isStream() {
        return false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + '}';
    }
}
