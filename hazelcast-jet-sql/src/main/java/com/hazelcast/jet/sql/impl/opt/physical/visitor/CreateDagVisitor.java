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

package com.hazelcast.jet.sql.impl.opt.physical.visitor;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.expression.ExpressionUtil;
import com.hazelcast.jet.sql.impl.opt.physical.FilterPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.InsertPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.JetRootRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.ProjectPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.ValuesPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientSourceP;
import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;
import static com.hazelcast.jet.sql.impl.processors.RootResultConsumerSink.rootResultConsumerSink;

public class CreateDagVisitor {

    private final DAG dag = new DAG();
    private final Address localMemberAddress;

    public CreateDagVisitor(Address localMemberAddress) {
        this.localMemberAddress = localMemberAddress;
    }

    public Vertex onValues(ValuesPhysicalRel rel) {
        List<Object[]> items = new ArrayList<>(rel.getTuples().size());
        for (List<RexLiteral> tuple : rel.getTuples()) {
            Object[] result = new Object[tuple.size()];
            for (int i = 0; i < tuple.size(); i++) {
                RexLiteral literal = tuple.get(i);

                Comparable<?> value = literal.getValue();
                if (value instanceof NlsString) {
                    NlsString nlsString = (NlsString) value;
                    assert nlsString.getCharset().name().equals(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
                    value = nlsString.getValue();
                }

                result[i] = rel.schema().getType(i).convert(value);
            }
            items.add(result);
        }

        return dag.newVertex("values-src", convenientSourceP(
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
    }

    public Vertex onInsert(InsertPhysicalRel rel) {
        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();

        Vertex vertex = getJetSqlConnector(table).sink(dag, table);
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    public Vertex onFullScan(FullScanPhysicalRel rel) {
        Table table = rel.getTableUnwrapped();

        return getJetSqlConnector(table)
                .fullScanReader(dag, table, null, rel.filter(), rel.projection());
    }

    public Vertex onFilter(FilterPhysicalRel rel) {
        FunctionEx<Object[], Object[]> filter = ExpressionUtil.filterFn(rel.filter());

        Vertex vertex = dag.newVertex("filter", mapP(filter::apply));
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    public Vertex onProject(ProjectPhysicalRel rel) {
        FunctionEx<Object[], Object[]> projection = ExpressionUtil.projectionFn(rel.projection());

        Vertex vertex = dag.newVertex("project", mapP(projection));
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    public Vertex onRoot(JetRootRel rootRel) {
        Vertex vertex = dag.newVertex("ClientSink",
                rootResultConsumerSink(rootRel.getInitiatorAddress(), rootRel.getQueryId()));

        // We use distribute-to-one edge to send all the items to the initiator member.
        // Such edge has to be partitioned, but the sink is LP=1 anyway, so we can use
        // allToOne with any key, it goes to a single processor on a single member anyway.
        connectInput(rootRel.getInput(), vertex,
                edge -> edge.allToOne("").distributeTo(localMemberAddress));
        return vertex;
    }

    public DAG getDag() {
        return dag;
    }

    /**
     * Converts the {@code inputRel} into a {@code Vertex} by visiting it and
     * create an edge from the input vertex into {@code thisVertex}.
     *
     * @param configureEdgeFn optional function to configure the edge
     */
    private void connectInput(RelNode inputRel, Vertex thisVertex,
                              @Nullable Consumer<Edge> configureEdgeFn) {
        Vertex inputVertex = ((PhysicalRel) inputRel).visit(this);
        Edge edge = between(inputVertex, thisVertex);
        if (configureEdgeFn != null) {
            configureEdgeFn.accept(edge);
        }
        dag.edge(edge);
    }
}
