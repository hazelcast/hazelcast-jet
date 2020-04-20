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

package com.hazelcast.jet.sql.parser;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlInsertKeyword;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class JetSqlInsert extends SqlInsert {

    private final SqlNodeList extendedKeywords;
    private final String tableName;

    public JetSqlInsert(SqlParserPos pos,
                        SqlNodeList keywords,
                        SqlNodeList extendedKeywords,
                        SqlNode targetTable,
                        SqlNode source,
                        SqlNodeList columnList) {
        super(pos, keywords, targetTable, source, columnList);
        this.extendedKeywords = extendedKeywords;
        if (targetTable instanceof SqlTableRef) {
            SqlTableRef tableRef = (SqlTableRef) targetTable;
            this.tableName = ((SqlIdentifier) tableRef.operand(0)).getSimple();
        } else {
            this.tableName = ((SqlIdentifier) targetTable).getSimple();
        }
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        String insertKeyword = "INSERT INTO";
        if (isUpsert()) {
            insertKeyword = "UPSERT INTO";
        } else if (isOverwrite()) {
            insertKeyword = "INSERT OVERWRITE";
        }
        writer.sep(insertKeyword);
        int opLeft = getOperator().getLeftPrec();
        int opRight = getOperator().getRightPrec();
        getTargetTable().unparse(writer, opLeft, opRight);
        if (getTargetColumnList() != null) {
            getTargetColumnList().unparse(writer, opLeft, opRight);
        }
        writer.newlineAndIndent();
        getSource().unparse(writer, 0, 0);
    }

    public static boolean isUpsert(List<SqlLiteral> keywords) {
        for (SqlNode keyword : keywords) {
            if (((SqlLiteral) keyword).symbolValue(SqlInsertKeyword.class) == SqlInsertKeyword.UPSERT) {
                return true;
            }
        }
        return false;
    }

    public boolean isOverwrite() {
        for (SqlNode keyword : extendedKeywords) {
            if (((SqlLiteral) keyword).symbolValue(JetSqlInsertKeyword.class) == JetSqlInsertKeyword.OVERWRITE) {
                return true;
            }
        }
        return false;
    }
}
