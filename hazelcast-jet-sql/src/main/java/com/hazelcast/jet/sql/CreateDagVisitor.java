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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.expression.RexToExpressionVisitor;
import com.hazelcast.jet.sql.imap.IMapProjectPhysicalRel;
import com.hazelcast.jet.sql.imap.IMapScanPhysicalRel;
import com.hazelcast.jet.sql.schema.JetTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.physical.FieldTypeProvider;
import com.hazelcast.sql.impl.physical.PhysicalNodeSchema;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;

import static com.hazelcast.jet.core.Edge.between;

public class CreateDagVisitor {

    private DAG dag;
    private Deque<VertexAndOrdinal> vertexStack = new ArrayDeque<>();

    public CreateDagVisitor(DAG dag, @Nullable Vertex sink) {
        this.dag = dag;
        if (sink != null) {
            vertexStack.push(new VertexAndOrdinal(sink));
        }
    }

    public void onConnectorFullScan(IMapScanPhysicalRel rel) {
        JetTable table = rel.getTableUnwrapped();
        PhysicalNodeSchema schema = new PhysicalNodeSchema(table.getPhysicalRowType());
        Expression<Boolean> predicate = convertFilter(schema, rel.getFilter());
        Tuple2<Vertex, Vertex> subDag = table.getSqlConnector().fullScanReader(dag, table, null, predicate,
                rel.getProjects());
        assert subDag != null : "null subDag"; // we check for this earlier TODO check for it earlier :)
        VertexAndOrdinal targetVertex = vertexStack.peek();
        assert targetVertex != null : "targetVertex=null";
        dag.edge(between(subDag.f1(), targetVertex.vertex));
        targetVertex.ordinal++;
    }

    @SuppressWarnings("unchecked")
    private Expression<Boolean> convertFilter(PhysicalNodeSchema schema, RexNode expression) {
        if (expression == null) {
            return null;
        }

        Expression convertedExpression = convertExpression(schema, expression);

        return (Expression<Boolean>) convertedExpression;
    }

    private Expression<?> convertExpression(FieldTypeProvider fieldTypeProvider, RexNode expression) {
        if (expression == null) {
            return null;
        }

        RexToExpressionVisitor converter = new RexToExpressionVisitor(fieldTypeProvider, 0);

        return expression.accept(converter);
    }

    public void onProject(IMapProjectPhysicalRel rel) {
        throw new UnsupportedOperationException("TODO");
    }

    public void onTableInsert(JetTableInsertPhysicalRel rel) {
        final JetTable jetTable = rel.getTable().unwrap(JetTable.class);
        Tuple2<Vertex, Vertex> sink = jetTable.getSqlConnector().sink(dag, jetTable);
        if (sink == null) {
            throw new JetException("This connector doesn't support writing");
        }
        vertexStack.push(new VertexAndOrdinal(sink.f0()));
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
