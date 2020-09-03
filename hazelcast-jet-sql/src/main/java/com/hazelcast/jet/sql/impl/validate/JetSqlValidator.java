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

import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.parse.SqlCreateJob;
import com.hazelcast.jet.sql.impl.parse.SqlExtendedInsert;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorTable;

import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;

public class JetSqlValidator extends HazelcastSqlValidator {

    private static final ValidatorResource RESOURCES = Resources.create(ValidatorResource.class);

    private boolean isCreateJob;

    public JetSqlValidator(
            SqlValidatorCatalogReader catalogReader,
            HazelcastTypeFactory typeFactory,
            SqlConformance conformance
    ) {
        super(JetSqlOperatorTable.instance(), catalogReader, typeFactory, conformance);
    }

    @Override
    public SqlNode validate(SqlNode topNode) {
        if (topNode instanceof SqlCreateJob) {
            isCreateJob = true;
        }

        if (topNode.getKind().belongsTo(SqlKind.DDL)) {
            topNode.validate(this, getEmptyScope());
            return topNode;
        }

        SqlNode validated = super.validate(topNode);

        if (validated instanceof SqlExtendedInsert) {
            if (!isCreateJob && isInsertFormStreamingSource((SqlExtendedInsert) validated)) {
                throw newValidationError(topNode, RESOURCES.mustUseCreateJob());
            }
            SqlExtendedInsert insert = ((SqlExtendedInsert) validated);
            HazelcastTable table = getCatalogReader().getTable(insert.tableNames()).unwrap(HazelcastTable.class);
            SqlConnector connector = getJetSqlConnector(table.getTarget());
            if (!insert.isOverwrite() && !connector.supportsPlainInserts()) {
                throw newValidationError(insert,
                        RESOURCES.plainInsertNotSupported(connector.getClass().getSimpleName()));
            }
        }
        return validated;
    }

    /**
     * Goes over all the referenced tables in the source query of the insert
     * statement and returns true if any of it is a streaming connector.
     */
    private boolean isInsertFormStreamingSource(SqlExtendedInsert insertNode) {
        class FindStreamingTablesVisitor extends SqlBasicVisitor<Void> {
            boolean found;

            @Override
            public Void visit(SqlIdentifier id) {
                SqlValidatorTable table = getCatalogReader().getTable(id.names);
                if (table != null) { // not every identifier is a table
                    HazelcastTable unwrappedTable = table.unwrap(HazelcastTable.class);
                    SqlConnector connector = getJetSqlConnector(unwrappedTable.getTarget());
                    if (connector.isStream()) {
                        found = true;
                    }
                }
                return super.visit(id);
            }
        }

        FindStreamingTablesVisitor visitor = new FindStreamingTablesVisitor();
        insertNode.getSource().accept(visitor);
        return visitor.found;
    }
}
