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
import com.hazelcast.jet.sql.impl.JetPlan.CreateExternalMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropExternalMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.ExecutionPlan;
import com.hazelcast.jet.sql.impl.calcite.parser.JetSqlParser;
import com.hazelcast.jet.sql.impl.convert.JetSqlToRelConverter;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.JetRootRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.jet.sql.impl.parse.SqlCreateExternalMapping;
import com.hazelcast.jet.sql.impl.parse.SqlCreateJob;
import com.hazelcast.jet.sql.impl.parse.SqlDropExternalMapping;
import com.hazelcast.jet.sql.impl.parse.SqlDropJob;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.jet.sql.impl.validate.JetSqlValidator;
import com.hazelcast.jet.sql.impl.validate.UnsupportedOperationVisitor;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.SqlBackend;
import com.hazelcast.sql.impl.calcite.parse.QueryConvertResult;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
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
import java.util.List;

import static java.util.stream.Collectors.toList;

class JetSqlBackend implements SqlBackend {

    private final NodeEngine nodeEngine;
    private final JetPlanExecutor planExecutor;

    JetSqlBackend(NodeEngine nodeEngine, JetPlanExecutor planExecutor) {
        this.nodeEngine = nodeEngine;
        this.planExecutor = planExecutor;
    }

    @Override
    public SqlParserImplFactory parserFactory() {
        return JetSqlParser.FACTORY;
    }

    @Override
    public SqlValidator validator(
            CatalogReader catalogReader,
            HazelcastTypeFactory typeFactory,
            SqlConformance sqlConformance
    ) {
        return new JetSqlValidator(catalogReader, typeFactory, sqlConformance);
    }

    @Override
    public SqlVisitor<Void> unsupportedOperationVisitor(CatalogReader catalogReader) {
        return UnsupportedOperationVisitor.INSTANCE;
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
                config
        );
    }

    @Override
    public SqlPlan createPlan(
            OptimizationTask task,
            QueryParseResult parseResult,
            OptimizerContext context
    ) {
        SqlNode node = parseResult.getNode();

        if (node instanceof SqlCreateExternalMapping) {
            return toCreateTablePlan((SqlCreateExternalMapping) node);
        } else if (node instanceof SqlDropExternalMapping) {
            return toDropTablePlan((SqlDropExternalMapping) node);
        } else if (node instanceof SqlCreateJob) {
            return toCreateJobPlan(parseResult, context);
        } else if (node instanceof SqlDropJob) {
            return toDropJobPlan((SqlDropJob) node);
        } else {
            QueryConvertResult convertResult = context.convert(parseResult);
            return toPlan(convertResult.getRel(), context);
        }
    }

    private SqlPlan toCreateTablePlan(SqlCreateExternalMapping sqlCreateTable) {
        List<MappingField> mappingFields = sqlCreateTable.columns()
                .map(field -> new MappingField(field.name(), field.type(), field.externalName()))
                .collect(toList());
        Mapping mapping = new Mapping(
                sqlCreateTable.name(),
                sqlCreateTable.type(),
                mappingFields,
                sqlCreateTable.options()
        );

        return new CreateExternalMappingPlan(
                mapping,
                sqlCreateTable.getReplace(),
                sqlCreateTable.ifNotExists(),
                planExecutor
        );
    }

    private SqlPlan toDropTablePlan(SqlDropExternalMapping sqlDropTable) {
        return new DropExternalMappingPlan(
                sqlDropTable.name(),
                sqlDropTable.ifExists(),
                planExecutor
        );
    }

    private SqlPlan toCreateJobPlan(QueryParseResult parseResult, OptimizerContext context) {
        SqlCreateJob node = (SqlCreateJob) parseResult.getNode();
        SqlNode source = node.dmlStatement();
        QueryParseResult newParseResult =
                new QueryParseResult(source, parseResult.getParameterRowType(), parseResult.getValidator(), this);
        QueryConvertResult convertedResult = context.convert(newParseResult);
        ExecutionPlan dmlPlan = toPlan(convertedResult.getRel(), context);
        return new CreateJobPlan(node.name(), node.jobConfig(), node.ifNotExists(), dmlPlan, planExecutor);
    }

    private SqlPlan toDropJobPlan(SqlDropJob node) {
        return new DropJobPlan(node.name(), node.ifExists(), planExecutor);
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

        return new ExecutionPlan(dag, isStreamRead[0], isInsert, queryId, rowMetadata, planExecutor);
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
        CreateDagVisitor visitor = new CreateDagVisitor(nodeEngine.getLocalMember().getAddress());
        physicalRel.visit(visitor);
        return visitor.getDag();
    }
}
