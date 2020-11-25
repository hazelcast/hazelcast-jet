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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.NestedLoopJoin;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY_PATH;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;

@RunWith(JUnitParamsRunner.class)
public class IMapSqlConnectorTest {

    private IMapSqlConnector connector;

    @Mock
    private DAG dag;

    @Mock
    private PartitionedMapTable table;

    @Mock
    private Vertex vertex;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        connector = new IMapSqlConnector();
    }

    @SuppressWarnings("unused")
    private Object[] joinTypes() {
        return new Object[]{new Object[]{true}, new Object[]{false}};
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByPrimitiveKey(boolean inner) {
        // given
        given(table.getFields()).willReturn(singletonList(new MapTableField("field", VARCHAR, false, KEY_PATH)));
        given(dag.newUniqueVertex(contains("Lookup"), isA(JoinByPrimitiveKeyProcessorSupplier.class))).willReturn(vertex);

        // when
        NestedLoopJoin join =
                connector.nestedLoopReader(dag, table, null, emptyList(), joinInfo(inner, new int[]{0}, new int[]{0}));

        // then
        assertThat(join.vertex()).isNotNull();
        assertThat(join.configureEdgeFn()).isNotNull();
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByPredicate(boolean inner) {
        // given
        given(table.getFields())
                .willReturn(singletonList(new MapTableField("field", VARCHAR, false, QueryPath.create("path"))));
        given(dag.newUniqueVertex(contains("Predicate"), isA(JoinByPredicateProcessorSupplier.class))).willReturn(vertex);

        // when
        NestedLoopJoin join =
                connector.nestedLoopReader(dag, table, null, emptyList(), joinInfo(inner, new int[]{0}, new int[]{0}));

        // then
        assertThat(join.vertex()).isNotNull();
        assertThat(join.configureEdgeFn()).isNull();
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByScan(boolean inner) {
        // given
        given(table.getFields()).willReturn(singletonList(new MapTableField("field", VARCHAR, false, KEY_PATH)));
        given(dag.newUniqueVertex(contains("Scan"), isA(JoinScanProcessorSupplier.class))).willReturn(vertex);

        // when
        NestedLoopJoin join =
                connector.nestedLoopReader(dag, table, null, emptyList(), joinInfo(inner, new int[0], new int[0]));

        // then
        assertThat(join.vertex()).isNotNull();
        assertThat(join.configureEdgeFn()).isNull();
    }

    private static JetJoinInfo joinInfo(boolean inner, int[] leftEquiJoinIndices, int[] rightEquiJoinIndices) {
        return new JetJoinInfo(inner, leftEquiJoinIndices, rightEquiJoinIndices, null, null);
    }
}
