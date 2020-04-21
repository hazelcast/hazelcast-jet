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
* Parser CREATE SERVER statement.
*/
SqlCreate JetSqlCreateServer(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    SqlIdentifier serverName;
    SqlNodeList properties = SqlNodeList.EMPTY;
    SqlIdentifier connector;
}
{
    <SERVER>
    serverName = CompoundIdentifier()
    [
        <OPTIONS>
        properties = ServerProperties()
    ]
    <CONNECTOR>
    connector = SimpleIdentifier()
    {
        return new JetSqlCreateServer(startPos.plus(getPos()),
                serverName,
                properties,
                connector,
                replace);
    }
}

SqlNodeList ServerProperties():
{
    Span span;
    SqlProperty property;
    Map<String, SqlNode> properties = new HashMap<String, SqlNode>();
}
{
    <LPAREN> { span = span(); }
    [
        property = ServerProperty()
        {
            properties.put(property.key(), property);
        }
        (
            <COMMA> property = ServerProperty()
            {
                if (properties.putIfAbsent(property.key(), property) != null) {
                    throw SqlUtil.newContextException(getPos(),
                        ParserResource.RESOURCE.duplicateProperty(property.key()));
                }
            }
        )*
    ]
    <RPAREN>
    {
        return new SqlNodeList(properties.values(), span.end(this));
    }
}

SqlProperty ServerProperty() :
{
    SqlIdentifier key;
    SqlNode value;
}
{
    key = SimpleIdentifier()
    value = StringLiteral()
    {
        return new SqlProperty(getPos(), key, value);
    }
}

/**
* Parser CREATE FOREIGN TABLE statement.
*/
SqlCreate JetSqlCreateTable(Span span, boolean replace) :
{
    SqlParserPos startPos = span.pos();
    SqlIdentifier tableName;
    SqlNodeList columns = SqlNodeList.EMPTY;
    SqlNodeList properties = SqlNodeList.EMPTY;
    SqlIdentifier server;
}
{
    <FOREIGN> <TABLE>
    tableName = CompoundIdentifier()
    columns = TableColumns()
    [
        <OPTIONS>
        properties = TableProperties()
    ]
    <SERVER>
    server = SimpleIdentifier()
    {
        return new JetSqlCreateTable(startPos.plus(getPos()),
                tableName,
                columns,
                properties,
                server,
                replace);
    }
}

SqlNodeList TableColumns():
{
    Span span;
    SqlTableColumn column;
    Map<String, SqlNode> columns = new HashMap<String, SqlNode>();
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
    SqlDataTypeSpec type;
}
{
    name = SimpleIdentifier()
    type = DataType()
    {
        return new SqlTableColumn(getPos(), name, type);
    }
}

SqlNodeList TableProperties():
{
    Span span;
    SqlProperty property;
    Map<String, SqlNode> properties = new HashMap<String, SqlNode>();
}
{
    <LPAREN> { span = span(); }
    [
        property = TableProperty()
        {
            properties.put(property.key(), property);
        }
        (
            <COMMA> property = TableProperty()
            {
                if (properties.putIfAbsent(property.key(), property) != null) {
                    throw SqlUtil.newContextException(getPos(),
                        ParserResource.RESOURCE.duplicateProperty(property.key()));
                }
            }
        )*
    ]
    <RPAREN>
    {
        return new SqlNodeList(properties.values(), span.end(this));
    }
}

SqlProperty TableProperty() :
{
    SqlIdentifier key;
    SqlNode value;
}
{
    key = SimpleIdentifier()
    value = StringLiteral()
    {
        return new SqlProperty(getPos(), key, value);
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
