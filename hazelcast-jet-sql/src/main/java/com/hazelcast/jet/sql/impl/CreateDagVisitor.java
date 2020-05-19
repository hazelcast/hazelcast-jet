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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.JetSqlConnector;
import com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector;
import com.hazelcast.jet.sql.impl.expression.ExpressionUtil;
import com.hazelcast.jet.sql.impl.expression.RexToExpressionVisitor;
import com.hazelcast.jet.sql.impl.rel.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.rel.NestedLoopJoinPhysicalRel;
import com.hazelcast.jet.sql.impl.rel.ProjectPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientSourceP;
import static com.hazelcast.jet.impl.util.Util.toList;

public class CreateDagVisitor {

    private final DAG dag;
    private final Deque<VertexAndOrdinal> vertexStack;

    public CreateDagVisitor(@Nonnull DAG dag, @Nullable Vertex sink) {
        this.dag = dag;
        this.vertexStack = new ArrayDeque<>();

        if (sink != null) {
            vertexStack.push(new VertexAndOrdinal(sink));
        }
    }

    public void onConnectorFullScan(FullScanPhysicalRel rel) {
        Table table = rel.getTableUnwrapped();
        PlanNodeSchema schema = new PlanNodeSchema(toList(table.getFields(), TableField::getType));
        Expression<Boolean> predicate = convertFilter(schema, rel.getFilter());
        List<Expression<?>> projection = rel.getProjection().stream()
                                            .map(projectNode -> convertExpression(schema, projectNode, rel.getProjection().size()))
                                            .collect(Collectors.toList());
        Vertex vertex = getJetSqlConnector(table)
                .fullScanReader(dag, table, null, predicate, projection);
        assert vertex != null : "null subDag"; // we check for this earlier TODO check for it earlier :)
        VertexAndOrdinal targetVertex = vertexStack.peek();
        assert targetVertex != null : "targetVertex=null";
        dag.edge(between(vertex, targetVertex.vertex));
        targetVertex.ordinal++;
    }

    @SuppressWarnings("unchecked")
    private Expression<Boolean> convertFilter(PlanNodeSchema schema, RexNode expression) {
        if (expression == null) {
            return null;
        }
        return (Expression<Boolean>) convertExpression(schema, expression, 0);
    }

    private Expression<?> convertExpression(PlanNodeFieldTypeProvider fieldTypeProvider, RexNode expression, int parameterCount) {
        if (expression == null) {
            return null;
        }
        RexToExpressionVisitor converter = new RexToExpressionVisitor(fieldTypeProvider, parameterCount);
        return expression.accept(converter);
    }

    public void onTableInsert(JetTableInsertPhysicalRel rel) {
        final HazelcastTable hzTable = rel.getTable().unwrap(HazelcastTable.class);
        Table table = hzTable.getTarget();
        JetSqlConnector connector = getJetSqlConnector(table);
        Vertex vertex = connector.sink(dag, table);
        if (vertex == null) {
            throw new JetException("This connector doesn't support writing");
        }
        vertexStack.push(new VertexAndOrdinal(vertex));
    }

    private JetSqlConnector getJetSqlConnector(Table table) {
        JetSqlConnector connector;
        if (table instanceof JetTable) {
            connector = ((JetTable) table).getSqlConnector();
        } else if (table instanceof PartitionedMapTable) {
            connector = new IMapSqlConnector();
        } else if (table instanceof ReplicatedMapTable) {
            throw new UnsupportedOperationException("Jet doesn't yet support writing to a ReplicatedMap");
        } else {
            throw new JetException("Unknown table type: " + table.getClass());
        }
        return connector;
    }

    public void onValues(JetValuesPhysicalRel rel) {
        List<Object[]> items = toList(rel.getTuples(), tuple -> tuple.stream().map(rexLiteral -> {
            Comparable<?> value = rexLiteral.getValue();
            if (value instanceof NlsString) {
                NlsString nlsString = (NlsString) value;
                assert nlsString.getCharset().name().equals(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
                return nlsString.getValue();
            }
            return value;
        }).toArray());

        Vertex vertex = dag.newVertex("values-src", convenientSourceP(
                pCtx -> null,
                (ignored, buf) -> {
                    items.forEach(buf::add);
                    buf.close();
                },
                ctx -> null,
                (ctx, states) -> {
                },
                ConsumerEx.noop(),
                1,
                true)
        );

        push(vertex);
    }

    public void onNestedLoopRead(NestedLoopJoinPhysicalRel rel) {
        FullScanPhysicalRel rightRel = (FullScanPhysicalRel) rel.getRight();

        Table table = rightRel.getTableUnwrapped();

        Vertex vertex = getJetSqlConnector(table)
                             .nestedLoopReader(dag, table, rightRel.filter(), rightRel.projection(), rel.condition());
        push(vertex);
    }

    public void onProject(ProjectPhysicalRel rel) {
        FunctionEx<Object[], Object[]> projection = ExpressionUtil.projectionFn(rel.projection());
        Vertex vertex = dag.newVertex("project", mapP(projection::apply));

        push(vertex);
    }

    private void push(Vertex vertex) {
        assert vertex != null : "null subDag"; // we check for this earlier TODO check for it earlier :)

        VertexAndOrdinal targetVertex = vertexStack.peek();
        assert targetVertex != null : "targetVertex=null";
        dag.edge(between(vertex, targetVertex.vertex));
        targetVertex.ordinal++;
        vertexStack.push(new VertexAndOrdinal(vertex));
    }

    private static final class VertexAndOrdinal {
        final Vertex vertex;
        int ordinal;

        private VertexAndOrdinal(Vertex vertex) {
            this.vertex = vertex;
        }

        @Override
        public String toString() {
            return "{vertex=" + vertex.getName() + ", ordinal=" + ordinal + '}';
        }
    }
}
