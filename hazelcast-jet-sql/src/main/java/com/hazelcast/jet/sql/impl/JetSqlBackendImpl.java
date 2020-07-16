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
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.JetRootRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
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
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;

@SuppressWarnings("unused") // used through reflection
public class JetSqlBackendImpl implements JetSqlBackend, ManagedService {

    private JetInstance jetInstance;
    private Map<QueryId, RootResultConsumer> resultConsumerRegistry;

    @SuppressWarnings("unused") // used through reflection
    public void initJetInstance(@Nonnull JetInstance jetInstance) {
        assert this.jetInstance == null;
        this.jetInstance = jetInstance;
        HazelcastInstanceImpl hzInstance = (HazelcastInstanceImpl) jetInstance.getHazelcastInstance();
        JetService jetService = hzInstance.node.nodeEngine.getService(JetService.SERVICE_NAME);
        resultConsumerRegistry = jetService.getResultConsumerRegistry();
    }

    @Override
    public Object operatorTable() {
        return JetSqlOperatorTable.instance();
    }

    @Override
    public BiFunction<Object, Object, Object> validator() {
        return JetSqlValidator::validate;
    }

    @Override
    public JetPlan optimizeAndCreatePlan(
            NodeEngine nodeEngine,
            Object context0,
            Object inputRel0,
            List<String> rootColumnNames
    ) {
        OptimizerContext context = (OptimizerContext) context0;
        RelNode inputRel = (RelNode) inputRel0;

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

        return new JetPlan(dag, isStreamRead[0], isInsert, queryId, rowMetadata);
    }

    @Override
    public SqlResult execute(SqlPlan plan0, List<Object> params, long timeout, int pageSize) {
        if (params != null && !params.isEmpty()) {
            throw new JetException("Query parameters not yet supported");
        }
        if (timeout > 0) {
            throw new JetException("Query timeout not supported");
        }
        JetPlan plan = (JetPlan) plan0;

        // register the resultConsumer
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
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }
}
