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
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;

import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;

// TODO: most probably should be moved to HazelcastSqlValidator when IMDG starts supporting inserts
public final class JetSqlValidator {

    private static final JetSqlValidatorResource RESOURCES = Resources.create(JetSqlValidatorResource.class);

    private JetSqlValidator() {
    }

    public static Object validate(
            Object catalogReader,
            Object node
    ) {
        CatalogReader catalogReader0 = (CatalogReader) catalogReader;

        if (node instanceof SqlExtendedInsert) {
            SqlExtendedInsert insert = ((SqlExtendedInsert) node);
            HazelcastTable table = catalogReader0.getTable(insert.tableNames()).unwrap(HazelcastTable.class);
            JetSqlConnector connector = getJetSqlConnector(table.getTarget());
            if (!insert.isOverwrite() && !connector.supportsPlainInserts()) {
                throw newValidationError(insert,
                        RESOURCES.plainInsertNotSupported(connector.getClass().getSimpleName()));
            }
        }
        return node;
    }

    private static CalciteContextException newValidationError(
            SqlNode node,
            Resources.ExInst<SqlValidatorException> e
    ) {
        SqlParserPos pos = node.getParserPosition();
        return SqlUtil.newContextException(pos, e);
    }
}
