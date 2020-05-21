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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.jet.sql.JetSqlConnector;
import com.hazelcast.sql.impl.calcite.parse.SqlExtendedInsert;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;

import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;

public class JetSqlValidator extends HazelcastSqlValidator {

    private static final JetSqlValidatorResource RESOURCES = Resources.create(JetSqlValidatorResource.class);

    public JetSqlValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            SqlConformance conformance
    ) {
        super(opTab, catalogReader, typeFactory, conformance);
    }

    @Override
    public SqlNode validate(SqlNode topNode) {
        SqlNode validated = super.validate(topNode);

        if (validated instanceof SqlExtendedInsert) {
            SqlExtendedInsert insert = ((SqlExtendedInsert) validated);
            HazelcastTable table = getCatalogReader().getTable(insert.tableNames()).unwrap(HazelcastTable.class);
            JetSqlConnector connector = getJetSqlConnector(table.getTarget());
            if (!insert.isOverwrite() && !connector.supportsPlainInserts()) {
                throw newValidationError(insert,
                        RESOURCES.plainInsertNotSupported(connector.getClass().getSimpleName()));
            }
        }
        return validated;
    }
}
