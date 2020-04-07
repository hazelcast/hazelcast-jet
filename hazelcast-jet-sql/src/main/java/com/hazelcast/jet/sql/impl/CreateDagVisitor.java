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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.expression.RexToExpressionVisitor;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientSourceP;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.stream.Collectors.toList;

public class CreateDagVisitor {

    private DAG dag;
    private Deque<VertexAndOrdinal> vertexStack = new ArrayDeque<>();

    public CreateDagVisitor(DAG dag, @Nullable Vertex sink) {
        this.dag = dag;
        if (sink != null) {
            vertexStack.push(new VertexAndOrdinal(sink));
        }
    }

    public void onConnectorFullScan(FullScanPhysicalRel rel) {
        JetTable table = rel.getTableUnwrapped();
        PlanNodeSchema schema = new PlanNodeSchema(table.getPhysicalRowType());
        Expression<Boolean> predicate = convertFilter(schema, rel.getFilter());
        List<Expression<?>> projection = rel.getProjectNodes().stream()
                                            .map(projectNode -> convertExpression(schema, projectNode, rel.getProjectNodes().size()))
                                            .collect(toList());
        Vertex vertex = table.getSqlConnector().fullScanReader(dag, table, null, predicate, projection);
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
        final JetTable jetTable = rel.getTable().unwrap(JetTable.class);
        Vertex vertex = jetTable.getSqlConnector().sink(dag, jetTable);
        if (vertex == null) {
            throw new JetException("This connector doesn't support writing");
        }
        vertexStack.push(new VertexAndOrdinal(vertex));
    }

    public void onValues(JetValuesPhysicalRel rel) {
        List<Object[]> items = toList(rel.getTuples(), tuple -> tuple.stream().map(rexLiteral -> {
            Comparable<?> v = rexLiteral.getValue();
            if (v instanceof NlsString) {
                NlsString nlsString = (NlsString) v;
                assert nlsString.getCharset().name().equals(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
                return nlsString.getValue();
            }
            return v;
        }).toArray());

        Vertex v = dag.newVertex("values-src", convenientSourceP(
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
                true));

        VertexAndOrdinal targetVertex = vertexStack.peek();
        assert targetVertex != null : "targetVertex=null";
        dag.edge(between(v, targetVertex.vertex));
        targetVertex.ordinal++;
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
