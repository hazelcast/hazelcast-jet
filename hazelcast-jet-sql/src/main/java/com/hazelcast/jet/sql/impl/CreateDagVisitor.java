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
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.expression.ExpressionUtil;
import com.hazelcast.jet.sql.impl.rel.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.rel.NestedLoopJoinPhysicalRel;
import com.hazelcast.jet.sql.impl.rel.ProjectPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

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

    public void onTableInsert(JetTableInsertPhysicalRel rel) {
        JetTable table = rel.getTable().unwrap(JetTable.class);

        Vertex vertex = table.getSqlConnector().sink(dag, table);
        if (vertex == null) {
            throw new JetException("This connector doesn't support writing");
        }

        vertexStack.push(new VertexAndOrdinal(vertex)); // TODO: unify, use push() ?
    }

    public void onConnectorFullScan(FullScanPhysicalRel rel) {
        JetTable table = rel.getTableUnwrapped();

        Vertex vertex = table.getSqlConnector()
                             .fullScanReader(dag, table, null, rel.filter(), rel.projection());
        push(vertex);
    }

    public void onNestedLoopRead(NestedLoopJoinPhysicalRel rel) {
        FullScanPhysicalRel rightRel = (FullScanPhysicalRel) rel.getRight();

        JetTable table = rightRel.getTableUnwrapped();

        Tuple2<Vertex, Vertex> vertices = table.getSqlConnector()
                                             .nestedLoopReader(dag, table, rel.condition(), rightRel.projection());
        assert vertices != null;
        push(vertices.f1());
        push(vertices.f0());
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
