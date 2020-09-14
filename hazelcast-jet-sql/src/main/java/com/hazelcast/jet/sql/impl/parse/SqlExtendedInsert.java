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

package com.hazelcast.jet.sql.impl.parse;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;
import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;

public class SqlExtendedInsert extends SqlInsert {

    private final SqlNodeList extendedKeywords;

    public SqlExtendedInsert(
            SqlNode table,
            SqlNode source,
            SqlNodeList keywords,
            SqlNodeList extendedKeywords,
            SqlNodeList columns,
            SqlParserPos pos
    ) {
        super(pos, keywords, table, source, columns);

        this.extendedKeywords = extendedKeywords;
    }

    public ImmutableList<String> tableNames() {
        return ((SqlIdentifier) getTargetTable()).names;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        if (isSink()) {
            writer.keyword("SINK INTO");
        } else {
            writer.keyword("INSERT INTO");
        }
        int opLeft = getOperator().getLeftPrec();
        int opRight = getOperator().getRightPrec();
        getTargetTable().unparse(writer, opLeft, opRight);
        if (getTargetColumnList() != null) {
            getTargetColumnList().unparse(writer, opLeft, opRight);
        }
        writer.newlineAndIndent();
        getSource().unparse(writer, 0, 0);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        super.validate(validator, scope);

        HazelcastTable table = validator.getCatalogReader().getTable(tableNames()).unwrap(HazelcastTable.class);
        SqlConnector connector = getJetSqlConnector(table.getTarget());
        if (isSink()) {
            if (!connector.supportsSink()) {
                throw validator.newValidationError(this, RESOURCE.sinkIntoNotSupported(connector.typeName()));
            }
        } else {
            if (!connector.supportsInsert()) {
                throw validator.newValidationError(this, RESOURCE.insertIntoNotSupported(connector.typeName()));
            }
        }
    }

    private boolean isSink() {
        for (SqlNode keyword : extendedKeywords) {
            if (((SqlLiteral) keyword).symbolValue(Keyword.class) == Keyword.SINK) {
                return true;
            }
        }
        return false;
    }

    public enum Keyword {
        SINK;

        public SqlLiteral symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }
    }
}
