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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.Util.toList;

/**
 * {@link Table} implementation for IMap.
 */
public class IMapTable extends JetTable {

    private final String mapName;
    private final List<Entry<String, QueryDataType>> fields;
    private final List<String> fieldNames;
    private final List<QueryDataType> fieldTypes;

    private final EntryWriter writer;

    public IMapTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull String mapName,
            @Nonnull List<Entry<String, QueryDataType>> fields,
            @Nonnull EntryWriter writer
    ) {
        super(sqlConnector);
        this.mapName = mapName;
        this.fields = fields;
        this.fieldNames = toList(fields, Entry::getKey);
        this.fieldTypes = toList(fields, Entry::getValue);

        this.writer = writer;
    }

    String getMapName() {
        return mapName;
    }

    EntryWriter getWriter() {
        return writer;
    }

    List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public boolean isStream() {
        return false;
    }

    @Override
    public List<QueryDataType> getPhysicalRowType() {
        return fieldTypes;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (Entry<String, QueryDataType> field : fields) {
            RelDataType type = typeFactory.createSqlType(SqlTypeName.ANY);

            builder.add(field.getKey(), type)
                   .nullable(true);
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + '}';
    }
}
