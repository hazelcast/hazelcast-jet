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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.sql.schema.JetSchema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.JetRootCalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Collections;
import java.util.Properties;

public class JetSqlService {

    private final JetInstance instance;
    private final JetSchema schema = new JetSchema();
    private final SqlValidator validator = createValidator();

    public JetSqlService(JetInstance instance) {
        this.instance = instance;
    }

    /**
     * Parse SQL statement.
     *
     * @param sql SQL string.
     * @return SQL tree.
     */
    public SqlNode parse(String sql) {
        SqlNode node;

        try {
            SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();

            parserConfig.setCaseSensitive(true);
            parserConfig.setUnquotedCasing(Casing.UNCHANGED);
            parserConfig.setQuotedCasing(Casing.UNCHANGED);
            parserConfig.setConformance(HazelcastSqlConformance.INSTANCE);

            SqlParser parser = SqlParser.create(sql, parserConfig.build());

            node = parser.parseStmt();
        } catch (Exception e) {
            throw new JetException("Failed to parse SQL: " + e, e);
        }

        return validator.validate(node);
    }

    public JetSchema getSchema() {
        return schema;
    }

    private SqlValidator createValidator() {
        SqlOperatorTable opTab = SqlStdOperatorTable.instance();

        HazelcastTypeFactory typeFactory = new HazelcastTypeFactory();
        CalciteConnectionConfig connectionConfig = createConnectionConfig();
        Prepare.CatalogReader catalogReader = createCatalogReader(typeFactory, connectionConfig, schema);

        return new HazelcastSqlValidator(
                opTab,
                catalogReader,
                typeFactory,
                HazelcastSqlConformance.INSTANCE
        );
    }

    private static CalciteConnectionConfig createConnectionConfig() {
        Properties properties = new Properties();

        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        properties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        return new CalciteConnectionConfigImpl(properties);
    }

    private static Prepare.CatalogReader createCatalogReader(
            JavaTypeFactory typeFactory,
            CalciteConnectionConfig config,
            JetSchema rootSchema
    ) {
        return new CalciteCatalogReader(
                new JetRootCalciteSchema(rootSchema),
                Collections.emptyList(),
                typeFactory,
                config
        );
    }
}
