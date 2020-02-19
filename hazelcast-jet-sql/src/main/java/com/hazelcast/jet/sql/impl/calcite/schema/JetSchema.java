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

package com.hazelcast.jet.sql.impl.calcite.schema;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Schema operating on registered sources/sinks
 */
public class JetSchema extends AbstractSchema {
    private final ConcurrentMap<String, Table> tableMap = new ConcurrentHashMap<>();

    public JetSchema() {
        tableMap.put("foo", new Table() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return null;
            }

            @Override
            public Statistic getStatistic() {
                return null;
            }

            @Override
            public TableType getJdbcTableType() {
                return null;
            }

            @Override
            public boolean isRolledUp(String column) {
                return false;
            }

            @Override
            public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
                return false;
            }
        });
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }
}
