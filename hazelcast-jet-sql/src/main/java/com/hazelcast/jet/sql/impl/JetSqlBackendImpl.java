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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.sql.impl.JetPlan.CreateExternalTablePlan;
import com.hazelcast.jet.sql.impl.JetPlan.ExecutionPlan;
import com.hazelcast.jet.sql.impl.JetPlan.RemoveExternalTablePlan;
import com.hazelcast.jet.sql.impl.convert.JetSqlToRelConverter;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.JetRootRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.jet.sql.impl.parse.SqlCreateExternalTable;
import com.hazelcast.jet.sql.impl.parse.SqlDropExternalTable;
import com.hazelcast.jet.sql.impl.parse.SqlTableColumn;
import com.hazelcast.jet.sql.impl.parse.UnsupportedOperationVisitor;
import com.hazelcast.jet.sql.impl.schema.ExternalCatalog;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.jet.sql.impl.schema.ExternalTable;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.validate.JetSqlValidator;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.calcite.JetSqlBackend;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.parse.QueryConvertResult;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.parser.JetSqlParser;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlToRelConverter.Config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class JetSqlBackendImpl implements JetSqlBackend {

    private final NodeEngine nodeEngine;
    private final JetSqlServiceImpl sqlService;

    private final ExternalCatalog catalog;

    JetSqlBackendImpl(NodeEngine nodeEngine, JetSqlServiceImpl sqlService, ExternalCatalog catalog) {
        this.sqlService = sqlService;
        this.nodeEngine = nodeEngine;

        this.catalog = catalog;
    }

    @Override
    public SqlParserImplFactory parserFactory() {
        return JetSqlParser.FACTORY;
    }

    @Override
    public SqlValidator validator(
            CatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            SqlConformance sqlConformance
    ) {
        return new JetSqlValidator(catalogReader, typeFactory, sqlConformance);
    }

    @Override
    public SqlVisitor<Void> unsupportedOperationVisitor(CatalogReader catalogReader) {
        return new UnsupportedOperationVisitor(catalogReader);
    }

    @Override
    public SqlToRelConverter converter(
            ViewExpander viewExpander,
            SqlValidator sqlValidator,
            CatalogReader catalogReader,
            RelOptCluster relOptCluster,
            SqlRexConvertletTable sqlRexConvertletTable,
            Config config
    ) {
        return new JetSqlToRelConverter(
                viewExpander,
                sqlValidator,
                catalogReader,
                relOptCluster,
                sqlRexConvertletTable,
                config,
                catalog
        );
    }

    @Override
    public SqlPlan createPlan(
            OptimizationTask task,
            QueryParseResult parseResult,
            OptimizerContext context
    ) {
        SqlNode node = parseResult.getNode();

        if (node instanceof SqlCreateExternalTable) {
            return toCreateTablePlan(
                    parseResult,
                    context
            );
        } else if (node instanceof SqlDropExternalTable) {
            return toRemoveTablePlan((SqlDropExternalTable) node);
        } else {
            QueryConvertResult convertResult = context.convert(parseResult);
            return toPlan(convertResult.getRel(), context);
        }
    }

    private SqlPlan toCreateTablePlan(
            QueryParseResult parseResult,
            OptimizerContext context
    ) {
        SqlCreateExternalTable node = (SqlCreateExternalTable) parseResult.getNode();
        SqlNode source = node.source();
        if (source == null) {
            List<ExternalField> externalFields = node.columns()
                                                     .map(field -> new ExternalField(field.name(), field.type(), field.externalName()))
                                                     .collect(toList());
            ExternalTable externalTable = new ExternalTable(node.name(), node.type(), externalFields, node.options());

            return new CreateExternalTablePlan(externalTable, node.getReplace(), node.ifNotExists(), sqlService);
        } else {
            QueryConvertResult convertedResult = context.convert(parseResult);

            // TODO: ExternalTable is already being created in JetSqlToRelConverter, any way to reuse it ???
            List<ExternalField> externalFields = new ArrayList<>();
            Iterator<SqlTableColumn> columns = node.columns().iterator();
            for (TableField field : convertedResult.getRel().getTable().unwrap(HazelcastTable.class).getTarget().getFields()) {
                SqlTableColumn column = columns.hasNext() ? columns.next() : null;

                String name = field.getName();
                QueryDataType type = field.getType();
                String externalName = column != null ? column.externalName() : null;

                externalFields.add(new ExternalField(name, type, externalName));
            }
            assert !columns.hasNext() : "there are too many columns specified";
            ExternalTable externalTable = new ExternalTable(node.name(), node.type(), externalFields, node.options());

            ExecutionPlan populateTablePlan = toPlan(convertedResult.getRel(), context);
            return new CreateExternalTablePlan(
                    externalTable,
                    node.getReplace(),
                    node.ifNotExists(),
                    populateTablePlan,
                    sqlService
            );
        }
    }

    private SqlPlan toRemoveTablePlan(SqlDropExternalTable sqlDropTable) {
        return new RemoveExternalTablePlan(sqlDropTable.name(), sqlDropTable.ifExists(), sqlService);
    }

    private ExecutionPlan toPlan(RelNode inputRel, OptimizerContext context) {
        System.out.println("before logical opt:\n" + RelOptUtil.toString(inputRel));
        LogicalRel logicalRel = optimizeLogical(context, inputRel);
        System.out.println("after logical opt:\n" + RelOptUtil.toString(logicalRel));
        PhysicalRel physicalRel = optimizePhysical(context, logicalRel);
        System.out.println("after physical opt:\n" + RelOptUtil.toString(physicalRel));

        // add a root sink if the current root is not TableModify
        QueryId queryId;
        boolean isInsert = physicalRel instanceof TableModify;
        if (isInsert) {
            queryId = null;
        } else {
            queryId = QueryId.create(nodeEngine.getLocalMember().getUuid());
            physicalRel = new JetRootRel(physicalRel, nodeEngine.getThisAddress(), queryId);
        }

        // check whether any table is a stream
        boolean[] isStreamRead = {false};
        RelVisitor findStreamScanVisitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    JetTable jetTable = node.getTable().unwrap(JetTable.class);
                    if (jetTable != null && jetTable.isStream()) {
                        isStreamRead[0] = true;
                    }
                }
            }
        };
        findStreamScanVisitor.go(inputRel);

        DAG dag = createDag(physicalRel);

        RelDataType rootRowType = physicalRel.getRowType();
        List<SqlColumnMetadata> columns = new ArrayList<>(rootRowType.getFieldCount());

        for (RelDataTypeField field : rootRowType.getFieldList()) {
            // TODO real type
            columns.add(QueryUtils.getColumnMetadata(field.getName(), QueryDataType.OBJECT));
        }

        SqlRowMetadata rowMetadata = new SqlRowMetadata(columns);

        return new ExecutionPlan(dag, isStreamRead[0], isInsert, queryId, rowMetadata, sqlService);
    }

    /**
     * Perform logical optimization.
     *
     * @param rel Original logical tree.
     * @return Optimized logical tree.
     */
    private LogicalRel optimizeLogical(OptimizerContext context, RelNode rel) {
        return (LogicalRel) context.optimize(rel, LogicalRules.getRuleSet(),
                OptUtils.toLogicalConvention(rel.getTraitSet()));
    }

    /**
     * Perform physical optimization.
     * This is where proper access methods and algorithms for joins and aggregations are chosen.
     *
     * @param rel Optimized logical tree.
     * @return Optimized physical tree.
     */
    private PhysicalRel optimizePhysical(OptimizerContext context, RelNode rel) {
        return (PhysicalRel) context.optimize(rel, PhysicalRules.getRuleSet(),
                OptUtils.toPhysicalConvention(rel.getTraitSet()));
    }

    private DAG createDag(PhysicalRel physicalRel) {
        CreateDagVisitor visitor = new CreateDagVisitor();
        physicalRel.visit(visitor);
        return visitor.getDag();
    }
}
