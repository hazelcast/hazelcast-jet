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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.NestedLoopJoin;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.extract.QueryPath;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY_PATH;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;

@RunWith(JUnitParamsRunner.class)
public class JoinerTest {

    @Mock
    private DAG dag;

    @Mock
    private Vertex ingress;

    @Mock
    private Vertex egress;

    @Mock
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @SuppressWarnings("unused")
    private Object[] joinTypes() {
        return new Object[]{new Object[]{true}, new Object[]{false}};
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByPrimitiveKey(boolean inner) {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{KEY_PATH});
        given(dag.newUniqueVertex(contains("Lookup"), isA(JoinByPrimitiveKeyProcessorSupplier.class))).willReturn(ingress);

        // when
        NestedLoopJoin join = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(inner, new int[]{0}, new int[]{0}),
                rightRowProjectorSupplier
        );

        // then
        assertThat(join.ingress()).isNotNull();
        assertThat(join.ingress()).isEqualTo(join.egress());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_joinByPredicateInner() {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{QueryPath.create("path")});
        given(dag.newUniqueVertex(contains("Broadcast"), isA(SupplierEx.class))).willReturn(ingress);
        given(ingress.localParallelism(1)).willReturn(ingress);
        given(dag.newUniqueVertex(contains("Predicate"), isA(ProcessorMetaSupplier.class))).willReturn(egress);

        // when
        NestedLoopJoin join = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(true, new int[]{0}, new int[]{0}),
                rightRowProjectorSupplier
        );

        // then
        assertThat(join.ingress()).isNotNull();
        assertThat(join.egress()).isNotNull();
        assertThat(join.ingress()).isNotEqualTo(join.egress());
    }

    @Test
    public void test_joinByPredicateOuter() {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{QueryPath.create("path")});
        given(dag.newUniqueVertex(contains("Predicate"), isA(JoinByPredicateOuterProcessorSupplier.class)))
                .willReturn(ingress);

        // when
        NestedLoopJoin join = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(false, new int[]{0}, new int[]{0}),
                rightRowProjectorSupplier
        );

        // then
        assertThat(join.ingress()).isNotNull();
        assertThat(join.ingress()).isEqualTo(join.egress());
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByScan(boolean inner) {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{VALUE_PATH});
        given(dag.newUniqueVertex(contains("Scan"), isA(JoinScanProcessorSupplier.class))).willReturn(ingress);

        // when
        NestedLoopJoin join = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(inner, new int[0], new int[0]),
                rightRowProjectorSupplier
        );

        // then
        assertThat(join.ingress()).isNotNull();
        assertThat(join.ingress()).isEqualTo(join.egress());
    }

    private static JetJoinInfo joinInfo(boolean inner, int[] leftEquiJoinIndices, int[] rightEquiJoinIndices) {
        return new JetJoinInfo(inner, leftEquiJoinIndices, rightEquiJoinIndices, null, null);
    }
}
