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

package com.hazelcast.jet.sql.impl.convert;

import com.hazelcast.jet.sql.impl.parse.SqlCreateExternalTable;
import com.hazelcast.jet.sql.impl.parse.SqlTableColumn;
import com.hazelcast.jet.sql.impl.schema.ExternalCatalog;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.jet.sql.impl.schema.ExternalTable;
import com.hazelcast.jet.sql.impl.schema.UnknownStatistic;
import com.hazelcast.sql.impl.calcite.HazelcastSqlToRelConverter;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;

public class JetSqlToRelConverter extends HazelcastSqlToRelConverter {

    private final ExternalCatalog catalog;

    public JetSqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,
            SqlValidator validator,
            CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config,
            ExternalCatalog catalog
    ) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);

        this.catalog = catalog;
    }

    @Override
    public RelRoot convertQuery(
            SqlNode query,
            boolean needsValidation,
            boolean top
    ) {
        if (query instanceof SqlCreateExternalTable) {
            return convertCreateTable((SqlCreateExternalTable) query);
        } else {
            return super.convertQuery(query, needsValidation, top);
        }
    }

    private RelRoot convertCreateTable(SqlCreateExternalTable create) {
        assert create.source() != null : "source cannot be null";

        RelNode convertedSource = super.convertQuery(create.source(), false, true).rel;
        RelDataType rowType = convertedSource.getRowType();

        List<ExternalField> externalFields = new ArrayList<>();
        Iterator<SqlTableColumn> columns = create.columns().iterator();
        for (RelDataTypeField relField : rowType.getFieldList()) {
            SqlTableColumn column = columns.hasNext() ? columns.next() : null;

            String name = column != null ? column.name() : relField.getName();
            QueryDataType type = SqlToQueryType.map(relField.getType().getSqlTypeName());
            String externalName = column != null ? column.externalName() : null;

            externalFields.add(new ExternalField(name, type, externalName));
        }
        assert !columns.hasNext() : "there are too many columns specified";
        ExternalTable externalTable = new ExternalTable(create.name(), create.type(), externalFields, create.options());

        Table table = catalog.toTable(externalTable);
        RelOptTableImpl relTable = RelOptTableImpl.create(
                catalogReader,
                rowType,
                asList(table.getSchemaName(), table.getSqlName()),
                new HazelcastTable(table, UnknownStatistic.INSTANCE),
                null
        );
        RelOptTable hazelcastRelTable = new HazelcastRelOptTable(relTable);

        LogicalTableModify logicalTableModify = LogicalTableModify.create(
                hazelcastRelTable,
                catalogReader,
                convertedSource,
                Operation.INSERT,
                null,
                null,
                false
        );

        return RelRoot.of(logicalTableModify, rowType, SqlKind.INSERT);
    }

    /*@Override
    protected void collectInsertTargets(
            SqlInsert call,
            final RexNode sourceRef,
            final List<String> targetColumnNames,
            List<RexNode> columnExprs) {

        HazelcastTable hazelcastTable = getTargetTable(call).unwrap(HazelcastTable.class);
        List<String> columnNames = new FilteringList(hazelcastTable);

        super.collectInsertTargets(call, sourceRef, columnNames, columnExprs);

        targetColumnNames.addAll(columnNames);
    }

    private static class FilteringList extends ArrayList<String> {

        private final HazelcastTable hazelcastTable;

        private FilteringList(HazelcastTable hazelcastTable) {
            this.hazelcastTable = hazelcastTable;
        }

        @Override
        public boolean add(String s) {
            return !hazelcastTable.isHidden(s) && super.add(s);
        }

        @Override
        public boolean addAll(Collection<? extends String> c) {
            boolean modified = false;
            for (String s : c) {
                modified |= add(s);
            }
            return modified;
        }
    }*/
}
