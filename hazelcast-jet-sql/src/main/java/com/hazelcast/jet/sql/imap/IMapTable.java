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

package com.hazelcast.jet.sql.imap;

import com.hazelcast.jet.sql.schema.JetTable;
import com.hazelcast.sql.impl.type.DataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * {@link Table} implementation for IMap.
 */
public class IMapTable extends JetTable {

    private final String mapName;
    private final List<HazelcastTableIndex> indexes;
    private final Map<String, DataType> columns;

    public IMapTable(
            @Nonnull String mapName,
            @Nonnull List<HazelcastTableIndex> indexes,
            @Nonnull Map<String, DataType> columns
    ) {
        this.mapName = mapName;
        this.columns = columns;
        this.indexes = indexes;
    }

    @Override
    public boolean isStream() {
        return false;
    }

    public DataType getFieldType(String fieldName) {
        return columns.getOrDefault(fieldName, DataType.LATE); // TODO do we need the LATE type?
    }

    public String getMapName() {
        return mapName;
    }

    public List<HazelcastTableIndex> getIndexes() {
        return indexes;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new IMapTableRelDataType(typeFactory, columns);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", indexes=" + indexes + '}';
    }
}
