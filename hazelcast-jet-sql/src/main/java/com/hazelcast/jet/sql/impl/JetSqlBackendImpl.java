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

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.JetService;
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
import com.hazelcast.jet.sql.impl.parse.SqlOption;
import com.hazelcast.jet.sql.impl.parse.SqlTableColumn;
import com.hazelcast.jet.sql.impl.schema.ExternalCatalog;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.jet.sql.impl.schema.ExternalTable;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.validate.JetSqlOperatorTable;
import com.hazelcast.jet.sql.impl.validate.JetSqlValidator;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.JetSqlBackend;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.SingleValueResult;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.parse.QueryConvertResult;
import com.hazelcast.sql.impl.calcite.parser.JetSqlParser;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;
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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter.Config;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.apache.calcite.sql.SqlKind.ARGUMENT_ASSIGNMENT;
import static org.apache.calcite.sql.SqlKind.COLLECTION_TABLE;
import static org.apache.calcite.sql.SqlKind.COLUMN_DECL;
import static org.apache.calcite.sql.SqlKind.CREATE_TABLE;
import static org.apache.calcite.sql.SqlKind.DROP_TABLE;
import static org.apache.calcite.sql.SqlKind.INSERT;
import static org.apache.calcite.sql.SqlKind.ROW;
import static org.apache.calcite.sql.SqlKind.VALUES;

@SuppressWarnings("unused") // used through reflection
// TODO: break down this class pls....
public class JetSqlBackendImpl implements JetSqlBackend, ManagedService {

    private static final Set<SqlKind> SUPPORTED_KINDS = new HashSet<SqlKind>() {{
        add(CREATE_TABLE);
        add(DROP_TABLE);
        add(COLUMN_DECL);
        add(ROW);
        add(VALUES);
        add(INSERT);
        add(COLLECTION_TABLE);
        add(ARGUMENT_ASSIGNMENT);
    }};

    private static final Set<SqlOperator> SUPPORTED_OPERATORS = new HashSet<SqlOperator>() {{
        add(SqlOption.OPERATOR);
        add(JetSqlOperatorTable.FILE);
    }};

    private JetInstance jetInstance;
    private NodeEngine nodeEngine;

    private Map<QueryId, RootResultConsumer> resultConsumerRegistry;

    private ExternalCatalog catalog;

    @SuppressWarnings("unused") // used through reflection
    public void initJetInstance(@Nonnull JetInstance jetInstance) {
        this.jetInstance = Objects.requireNonNull(jetInstance);
        this.nodeEngine = ((HazelcastInstanceImpl) jetInstance.getHazelcastInstance()).node.nodeEngine;

        JetService jetService = nodeEngine.getService(JetService.SERVICE_NAME);
        this.resultConsumerRegistry = jetService.getResultConsumerRegistry();

        this.catalog = new ExternalCatalog(nodeEngine);
    }

    @Override
    public Object tableResolver() {
        return catalog;
    }

    @Override
    public Object kinds() {
        return SUPPORTED_KINDS;
    }

    @Override
    public Object operators() {
        return SUPPORTED_OPERATORS;
    }

    @Override
    public Object createParserFactory() {
        return JetSqlParser.FACTORY;
    }

    @Override
    public Object createValidator(Object catalogReader, Object typeFactory, Object conformance) {
        return new JetSqlValidator(
                (SqlValidatorCatalogReader) catalogReader,
                (RelDataTypeFactory) typeFactory,
                (SqlConformance) conformance
        );
    }

    @Override
    public Object createConverter(
            Object viewExpander,
            Object validator,
            Object catalogReader,
            Object cluster,
            Object convertletTable,
            Object config
    ) {
        return new JetSqlToRelConverter(
                (ViewExpander) viewExpander,
                (SqlValidator) validator,
                (CatalogReader) catalogReader,
                (RelOptCluster) cluster,
                (SqlRexConvertletTable) convertletTable,
                (Config) config,
                catalog
        );
    }

    @Override
    public Object createPlan(
            Object node0,
            Object parameterRowType0,
            Object context0
    ) {
        OptimizerContext context = (OptimizerContext) context0;
        RelDataType parameterRowType = (RelDataType) parameterRowType0;
        SqlNode node = (SqlNode) node0;

        if (node instanceof SqlCreateExternalTable) {
            return toCreateTablePlan(
                    (SqlCreateExternalTable) node,
                    parameterRowType,
                    context
            );
        } else if (node instanceof SqlDropExternalTable) {
            return toRemoveTablePlan((SqlDropExternalTable) node);
        } else {
            QueryConvertResult convertResult = context.convert(node);
            return toPlan(convertResult.getRel(), context);
        }
    }

    private SqlPlan toCreateTablePlan(
            SqlCreateExternalTable node,
            RelDataType parameterRowType,
            OptimizerContext context
    ) {
        SqlNode source = node.source();
        if (source == null) {
            List<ExternalField> externalFields = node.columns()
                .map(field -> new ExternalField(field.name(), field.type(), field.externalName()))
                .collect(toList());
            ExternalTable externalTable = new ExternalTable(node.name(), node.type(), externalFields, node.options());

            return new CreateExternalTablePlan(externalTable, node.getReplace(), node.ifNotExists(), this);
        } else {
            QueryConvertResult convertedResult = context.convert(node);

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
                    this
            );
        }
    }

    private SqlPlan toRemoveTablePlan(SqlDropExternalTable sqlDropTable) {
        return new RemoveExternalTablePlan(sqlDropTable.name(), sqlDropTable.ifExists(), this);
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

        return new ExecutionPlan(dag, isStreamRead[0], isInsert, queryId, rowMetadata, this);
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

    @Override
    public SqlResult execute(SqlPlan plan, List<Object> params, long timeout, int pageSize) {
        if (params != null && !params.isEmpty()) {
            throw new JetException("Query parameters not yet supported");
        }
        if (timeout > 0) {
            throw new JetException("Query timeout not supported");
        }

        return ((JetPlan) plan).execute(params, timeout, pageSize);
    }

    SqlResult execute(ExecutionPlan plan) {
        RootResultConsumer consumer;
        if (plan.isInsert()) {
            consumer = null;
        } else {
            consumer = new BlockingRootResultConsumer();
            consumer.setup(() -> { });
            Object oldValue = resultConsumerRegistry.put(plan.getQueryId(), consumer);
            assert oldValue == null : oldValue;
        }

        // submit the job
        Job job = jetInstance.newJob(plan.getDag());

        if (plan.isInsert()) {
            if (plan.isStreaming()) {
                return new SingleValueResult(job.getId());
            } else {
                job.join();
                // TODO return real updated row count
                return new SingleValueResult(-1L);
            }
        } else {
            return new JetSqlResultImpl(plan.getQueryId(), consumer, plan.getRowMetadata());
        }
    }

    SqlResult execute(CreateExternalTablePlan plan) {
        catalog.createTable(plan.schema(), plan.replace(), plan.ifNotExists());
        return new SingleValueResult(0L);
    }

    SqlResult execute(RemoveExternalTablePlan plan) {
        catalog.removeTable(plan.name(), plan.ifExists());
        return new SingleValueResult(0L);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }
}
