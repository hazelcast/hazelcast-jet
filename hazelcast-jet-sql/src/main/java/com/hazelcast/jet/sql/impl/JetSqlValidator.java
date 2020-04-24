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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.parser.JetSqlInsert;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class JetSqlValidator extends SqlValidatorImpl {

    private final SqlValidatorCatalogReader catalog;

    public JetSqlValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            SqlConformance conformance
    ) {
        super(opTab, catalogReader, typeFactory, conformance);
        this.catalog = catalogReader;
    }

    @Override
    public SqlNode validate(SqlNode topNode) {
        SqlNode validated = super.validate(topNode);
        // TODO: not sure if this is the right place for it...
        if (validated instanceof JetSqlInsert) {
            JetSqlInsert insert = ((JetSqlInsert) validated);
            JetTable table = (JetTable) catalog.getRootSchema().getTable(insert.tableName(), true).getTable();
            if (!insert.isOverwrite() && !table.getSqlConnector().supportsPlainInserts()) {
                throw new JetException(table.getSqlConnector().getClass().getSimpleName() +
                        " does not support plain INSERT INTO statement."); // TODO: wording
            }
        }
        return validated;
    }
}
