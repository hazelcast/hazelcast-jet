<#--
// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

/**
* Parses CREATE FOREIGN [DATA WRAPPER|TABLE] statement.
*/
SqlCreate JetSqlCreateConnectorOrTable(Span span, boolean replace) :
{
    SqlCreate create;
}
{
    (LOOKAHEAD(2)
        create = JetSqlCreateConnector(span, replace)
    |
        create = JetSqlCreateTable(span, replace)
    )
    {
        return create;
    }
}

/**
* Parses CREATE FOREIGN DATA WRAPPER statement.
*/
SqlCreate JetSqlCreateConnector(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    SqlIdentifier connectorName;
    SqlNodeList connectorOptions = SqlNodeList.EMPTY;
}
{
    <FOREIGN> <DATA> <WRAPPER>
    connectorName = SimpleIdentifier()
    [
        <LANGUAGE> <JAVA>
    ]
    [
        <OPTIONS>
        connectorOptions = GenericOptions()
    ]
    {
        return new JetSqlCreateConnector(startPos.plus(getPos()),
                connectorName,
                connectorOptions,
                replace);
    }
}

/**
* Parses CREATE SERVER statement.
*/
SqlCreate JetSqlCreateServer(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    SqlIdentifier serverName;
    SqlIdentifier connector;
    SqlNodeList serverOptions = SqlNodeList.EMPTY;
}
{
    <SERVER>
    serverName = SimpleIdentifier()
    <FOREIGN> <DATA> <WRAPPER>
    connector = SimpleIdentifier()
    [
        <OPTIONS>
        serverOptions = GenericOptions()
    ]
    {
        return new JetSqlCreateServer(startPos.plus(getPos()),
                serverName,
                connector,
                serverOptions,
                replace);
    }
}

/**
* Parses CREATE FOREIGN TABLE statement.
*/
SqlCreate JetSqlCreateTable(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    SqlIdentifier tableName;
    SqlNodeList columns = SqlNodeList.EMPTY;
    SqlIdentifier server;
    SqlNodeList tableOptions = SqlNodeList.EMPTY;
}
{
    <FOREIGN> <TABLE>
    tableName = SimpleIdentifier()
    columns = TableColumns()
    <SERVER>
    server = SimpleIdentifier()
    [
        <OPTIONS>
        tableOptions = GenericOptions()
    ]
    {
        return new JetSqlCreateTable(startPos.plus(getPos()),
                tableName,
                columns,
                server,
                tableOptions,
                replace);
    }
}

SqlNodeList TableColumns():
{
    Span span;
    SqlTableColumn column;
    Map<String, SqlNode> columns = new LinkedHashMap<String, SqlNode>();
}
{
    <LPAREN> { span = span(); }
    column = TableColumn()
    {
        columns.put(column.name(), column);
    }
    (
        <COMMA> column = TableColumn()
        {
            if (columns.putIfAbsent(column.name(), column) != null) {
               throw SqlUtil.newContextException(getPos(),
                   ParserResource.RESOURCE.duplicateColumn(column.name()));
            }
        }
    )*
    <RPAREN>
    {
        return new SqlNodeList(columns.values(), span.end(this));
    }
}

SqlTableColumn TableColumn() :
{
    SqlIdentifier name;
    SqlDataType type;
}
{
    name = SimpleIdentifier()
    type = SqlDataType()
    {
        return new SqlTableColumn(getPos(), name, type);
    }
}

SqlDataType SqlDataType() :
{
    Span span = Span.of();
    QueryDataType type;
}
{
    type = QueryDataType()
    {
        return new SqlDataType(span.end(this), type);
    }
}

QueryDataType QueryDataType() :
{
    QueryDataType type;
}
{
    (
        type = NumericType()
    |
        type = CharacterType()
    |
        type = DateTimeType()
    )
    {
        return type;
    }
}

QueryDataType NumericType() :
{
    int precision = -1;
    int scale = -1;
    QueryDataType type;
}
{
    (
        <BOOLEAN> { type = QueryDataType.BOOLEAN; }
    |
        <TINYINT> { type = QueryDataType.TINYINT; }
    |
        <SMALLINT> { type = QueryDataType.SMALLINT; }
    |
        (<INTEGER> | <INT>) { type = QueryDataType.INT; }
    |
        <BIGINT> { type = QueryDataType.BIGINT; }
    |
        (<REAL> | <FLOAT>) { type = QueryDataType.REAL; }
    |
        <DOUBLE> [ <PRECISION> ] { type = QueryDataType.DOUBLE; }
    |
        (<DECIMAL> | <DEC> | <NUMERIC>)
        [
            <LPAREN>
            precision = UnsignedIntLiteral()
            [
                <COMMA>
                scale = UnsignedIntLiteral()
            ]
            <RPAREN>
        ] { type = scale > 0 ? QueryDataType.DECIMAL : QueryDataType.DECIMAL_BIG_INTEGER; }
    )
    {
        return type;
    }
}

QueryDataType CharacterType() :
{
    QueryDataType type;
}
{
    (
        (<CHARACTER> | <CHAR>)
        (
            <VARYING> { type = QueryDataType.VARCHAR; }
        |
            { type = QueryDataType.VARCHAR_CHARACTER; }
        )
    |
        <VARCHAR> { type = QueryDataType.VARCHAR; }
    )
    {
        return type;
    }
}

QueryDataType DateTimeType() :
{
    SqlIdentifier variant = null;
    QueryDataType type;
}
{
    (
        <TIME> { type = QueryDataType.TIME; }
    |
        <DATE> { type = QueryDataType.DATE; }
    |
        <TIMESTAMP>
        (
            <WITH>
            (
                <TIME> <ZONE>
                [
                    <LPAREN> variant = SimpleIdentifier() <RPAREN>
                ]
                {
                    if (variant == null) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
                    } else if ("ZONED_DATE_TIME".equalsIgnoreCase(variant.getSimple())) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME;
                    } else if ("CALENDAR".equalsIgnoreCase(variant.getSimple())) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR;
                    } else {
                        throw SqlUtil.newContextException(getPos(),
                            ParserResource.RESOURCE.unknownTimestampVariant(variant.getSimple()));
                    }
                }
            |
                <LOCAL> <TIME> <ZONE>
                [
                    <LPAREN> variant = SimpleIdentifier() <RPAREN>
                ]
                {
                    if (variant == null) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_INSTANT;
                    } else if ("DATE".equalsIgnoreCase(variant.getSimple())) {
                        type = QueryDataType.TIMESTAMP_WITH_TZ_DATE;
                    } else {
                        throw SqlUtil.newContextException(getPos(),
                            ParserResource.RESOURCE.unknownTimestampVariant(variant.getSimple()));
                    }
                }
            )
        |
            <WITHOUT> <TIME> <ZONE> { type = QueryDataType.TIMESTAMP; }
        |
            { type = QueryDataType.TIMESTAMP; }
        )
    )
    {
        return type;
    }
}

/**
* Parses OPTIONS.
*/
SqlNodeList GenericOptions():
{
    Span span;
    SqlOption sqlOption;
    Map<String, SqlNode> sqlOptions = new LinkedHashMap<String, SqlNode>();
}
{
    <LPAREN> { span = span(); }
    [
        sqlOption = GenericOption()
        {
            sqlOptions.put(sqlOption.key(), sqlOption);
        }
        (
            <COMMA> sqlOption = GenericOption()
            {
                if (sqlOptions.putIfAbsent(sqlOption.key(), sqlOption) != null) {
                    throw SqlUtil.newContextException(getPos(),
                        ParserResource.RESOURCE.duplicateOption(sqlOption.key()));
                }
            }
        )*
    ]
    <RPAREN>
    {
        return new SqlNodeList(sqlOptions.values(), span.end(this));
    }
}

SqlOption GenericOption() :
{
    SqlIdentifier key;
    SqlNode value;
}
{
    key = SimpleIdentifier()
    value = StringLiteral()
    {
        return new SqlOption(getPos(), key, value);
    }
}

/**
* Parses DROP FOREIGN [DATA WRAPPER|TABLE] statement.
*/
SqlDrop JetSqlDropConnectorOrTable(Span span, boolean replace) :
{
    SqlDrop drop;
}
{
    (LOOKAHEAD(2)
        drop = JetSqlDropConnector(span, replace)
    |
        drop = JetSqlDropTable(span, replace)
    )
    {
        return drop;
    }
}

/**
* Parses DROP FOREIGN DATA WRAPPER statement.
*/
SqlDrop JetSqlDropConnector(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    SqlIdentifier connectorName;
    boolean cascade = false;
}
{
    <FOREIGN> <DATA> <WRAPPER>
    connectorName = SimpleIdentifier()
    [
        <CASCADE> { cascade = true; }
    |
        <RESTRICT>
    ]
    {
        return new JetSqlDropConnector(startPos.plus(getPos()),
                connectorName, cascade);
    }
}

/**
* Parses DROP SERVER statement.
*/
SqlDrop JetSqlDropServer(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    SqlIdentifier serverName;
    boolean cascade = false;
}
{
    <SERVER>
    serverName = SimpleIdentifier()
    [
        <CASCADE> { cascade = true; }
    |
        <RESTRICT>
    ]
    {
        return new JetSqlDropServer(startPos.plus(getPos()),
                serverName, cascade);
    }
}

/**
* Parses DROP FOREIGN TABLE statement.
*/
SqlDrop JetSqlDropTable(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    SqlIdentifier tableName;
}
{
    <FOREIGN> <TABLE>
    tableName = SimpleIdentifier()
    {
        return new JetSqlDropTable(startPos.plus(getPos()),
                tableName);
    }
}

/**
* Parses an extended INSERT statement.
*/
SqlNode JetSqlInsert() :
{
    Span span;
    SqlNode table;
    SqlNode source;
    List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    SqlNodeList keywordList;
    List<SqlLiteral> extendedKeywords = new ArrayList<SqlLiteral>();
    SqlNodeList extendedKeywordList;
    SqlNodeList extendList = null;
    SqlNodeList columnList = null;
}
{
    (
        <INSERT>
    |
        <UPSERT> { keywords.add(SqlInsertKeyword.UPSERT.symbol(getPos())); }
    )
    (
        <INTO>
    |
        <OVERWRITE> {
            if (JetSqlInsert.isUpsert(keywords)) {
                throw SqlUtil.newContextException(getPos(),
                    ParserResource.RESOURCE.overwriteIsOnlyUsedWithInsert());
            }
            extendedKeywords.add(JetSqlInsertKeyword.OVERWRITE.symbol(getPos()));
        }
    )
    { span = span(); }
    SqlInsertKeywords(keywords) {
        keywordList = new SqlNodeList(keywords, span.addAll(keywords).pos());
        extendedKeywordList = new SqlNodeList(extendedKeywords, span.addAll(extendedKeywords).pos());
    }
    table = TableRefWithHintsOpt()
    [
        LOOKAHEAD(5)
        [ <EXTEND> ]
        extendList = ExtendList() {
            table = extend(table, extendList);
        }
    ]
    [
        LOOKAHEAD(2)
        { Pair<SqlNodeList, SqlNodeList> p; }
        p = ParenthesizedCompoundIdentifierList() {
            if (p.right.size() > 0) {
                table = extend(table, p.right);
            }
            if (p.left.size() > 0) {
                columnList = p.left;
            }
        }
    ]
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new JetSqlInsert(span.end(source), table, source, keywordList, extendedKeywordList, columnList);
    }
}
