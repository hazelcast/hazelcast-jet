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

import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.validate.JetSqlValidator;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.impl.JetSqlBackend;
import com.hazelcast.sql.impl.SingleValueCursor;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Properties;

@SuppressWarnings("unused") // used through reflection
public class JetSqlBackendImpl implements JetSqlBackend, ManagedService {

    private JetInstance jetInstance;

    @SuppressWarnings("unused") // used through reflection
    public void initJetInstance(@Nonnull JetInstance jetInstance) {
        assert this.jetInstance == null;
        this.jetInstance = jetInstance;
    }

    @Override
    public JetSqlValidator createValidator(Object opTab, Object catalogReader, Object typeFactory, Object conformance) {
        return new JetSqlValidator((SqlOperatorTable) opTab, (SqlValidatorCatalogReader) catalogReader,
                (RelDataTypeFactory) typeFactory, (SqlConformance) conformance);
    }

    @Override
    public JetPlan optimizeAndCreatePlan(Object context0, Object inputRel0) {
        OptimizerContext context = (OptimizerContext) context0;
        RelNode inputRel = (RelNode) inputRel0;

        System.out.println("before logical opt:\n" + RelOptUtil.toString(inputRel));
        LogicalRel logicalRel = optimizeLogical(context, inputRel);
        System.out.println("after logical opt:\n" + RelOptUtil.toString(logicalRel));
        PhysicalRel physicalRel = optimizePhysical(context, logicalRel);
        System.out.println("after physical opt:\n" + RelOptUtil.toString(physicalRel));

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

        DAG dag;
        String observableName = null;
        int cursorColumnCount = 0;
        boolean isDml = inputRel instanceof TableModify;
        if (isDml) {
            dag = createDag(physicalRel, null);
        } else {
            observableName = "sql-sink-" + UuidUtil.newUnsecureUuidString();
            dag = createDag(physicalRel, SinkProcessors.writeObservableP(observableName));
            cursorColumnCount = physicalRel.getRowType().getFieldCount();
        }

        return new JetPlan(dag, isStreamRead[0], isDml, observableName, cursorColumnCount);
    }

    @Override
    public SqlCursor execute(SqlPlan plan0, List<Object> params, long timeout, int pageSize) {
        if (params != null && !params.isEmpty()) {
            throw new JetException("Query parameters not yet supported");
        }
        if (timeout > 0) {
            throw new JetException("Query timeout not supported");
        }
        JetPlan plan = (JetPlan) plan0;

        // submit the job
        Job job = jetInstance.newJob(plan.getDag());

        if (plan.isInsert()) {
            if (plan.isStreaming()) {
                return new SingleValueCursor(job.getId());
            } else {
                job.join();
                // TODO return real updated row count
                return new SingleValueCursor(-1L);
            }
        } else {
            // TODO will not run on a client
            return new SqlCursorFromObservable(plan.getCursorColumnCount(),
                    jetInstance.getObservable(plan.getObservableName()));
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

    private DAG createDag(PhysicalRel physicalRel, ProcessorMetaSupplier sinkSupplier) {
        DAG dag = new DAG();
        Vertex sink = sinkSupplier != null ? dag.newVertex("sink", sinkSupplier) : null;

        CreateDagVisitor visitor = new CreateDagVisitor(dag, sink);
        physicalRel.visit(visitor);
        return dag;
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
