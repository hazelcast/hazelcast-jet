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

import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.schema.JetTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
    private final List<HazelcastTableIndex> indexes;
    private final List<Entry<String, QueryDataType>> fields;
    private final String keyClassName;
    private final String valueClassName;

    private final List<String> fieldNames;
    private final List<QueryDataType> fieldTypes;

    public IMapTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull String mapName,
            @Nonnull List<HazelcastTableIndex> indexes,
            @Nonnull List<Entry<String, QueryDataType>> fields,
            String keyClassName,
            String valueClassName
    ) {
        super(sqlConnector);
        this.mapName = mapName;
        this.fields = fields;
        this.indexes = indexes;
        this.keyClassName = keyClassName;
        this.valueClassName = valueClassName;

        fieldNames = toList(fields, Entry::getKey);
        fieldTypes = toList(fields, Entry::getValue);
    }

    @Override
    public boolean isStream() {
        return false;
    }

    @Override
    public List<QueryDataType> getPhysicalRowType() {
        return fieldTypes;
    }

    public String getMapName() {
        return mapName;
    }

    public List<HazelcastTableIndex> getIndexes() {
        return indexes;
    }

    public List<Entry<String, QueryDataType>> getFields() {
        return fields;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new IMapRowRelDataType(typeFactory, fields);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", indexes=" + indexes + '}';
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public String getKeyClassName() {
        return keyClassName;
    }

    public String getValueClassName() {
        return valueClassName;
    }
}
