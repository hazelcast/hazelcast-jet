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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.IMap;
import com.hazelcast.projection.Projection;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class JoinScanProcessorTest {

    @Mock
    private IMap<Object, Object> map;

    @Mock
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    @Mock
    private ProcSupplierCtx supplierContext;

    @Mock
    private Context processorContext;

    @Mock
    private JetInstance jetInstance;

    @Mock
    private Outbox outbox;

    @Before
    public void setUp() {
        given(rightRowProjectorSupplier.columnCount()).willReturn(2);
        given(supplierContext.jetInstance()).willReturn(jetInstance);
        given(jetInstance.getMap(anyString())).willReturn(map);
        given(outbox.offer(anyInt(), any())).willReturn(true);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_innerJoin() throws Exception {
        // given
        Processor processor = processor((Expression<Boolean>) ConstantExpression.create(true, BOOLEAN), true);

        given(map.project(any(Projection.class))).willReturn(asList(null, new Object[]{10, "value-10"}));

        // when
        processor.process(0, new TestInbox(asList(new Object[]{1}, new Object[]{2})));
        processor.complete();

        // then
        verify(outbox).offer(-1, new Object[]{1, 10, "value-10"});
        verify(outbox).offer(-1, new Object[]{2, 10, "value-10"});
        verifyNoMoreInteractions(outbox);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_innerJoinFilteredOutByCondition_then_absent() throws Exception {
        // given
        Processor processor = processor(ComparisonPredicate.create(
                ColumnExpression.create(0, INT),
                ColumnExpression.create(1, INT),
                ComparisonMode.EQUALS
        ), true);

        given(map.project(any(Projection.class)))
                .willReturn(asList(new Object[]{1, "value-1"}, new Object[]{2, "value-2"}));

        // when
        processor.process(0, new TestInbox(asList(new Object[]{0}, new Object[]{1})));
        processor.complete();

        // then
        verify(outbox).offer(-1, new Object[]{1, 1, "value-1"});
        verifyNoMoreInteractions(outbox);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_outerJoin() throws Exception {
        // given
        Processor processor = processor((Expression<Boolean>) ConstantExpression.create(true, BOOLEAN), false);

        given(map.project(any(Projection.class))).willReturn(singletonList(null));

        // when
        processor.process(0, new TestInbox(asList(new Object[]{1}, new Object[]{2})));
        processor.complete();

        // then
        verify(outbox).offer(-1, new Object[]{1, null, null});
        verify(outbox).offer(-1, new Object[]{2, null, null});
        verifyNoMoreInteractions(outbox);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_outerJoinFilteredOutByCondition_then_nulls() throws Exception {
        // given
        Processor processor = processor(ComparisonPredicate.create(
                ColumnExpression.create(0, INT),
                ColumnExpression.create(1, INT),
                ComparisonMode.EQUALS
        ), false);

        given(map.project(any(Projection.class)))
                .willReturn(asList(new Object[]{1, "value-1"}, new Object[]{2, "value-2"}));

        // when
        processor.process(0, new TestInbox(asList(new Object[]{0}, new Object[]{1})));
        processor.complete();

        // then
        verify(outbox).offer(-1, new Object[]{0, null, null});
        verify(outbox).offer(-1, new Object[]{1, 1, "value-1"});
        verifyNoMoreInteractions(outbox);
    }

    private Processor processor(Expression<Boolean> condition, boolean inner) throws Exception {
        ProcessorSupplier supplier = new JoinScanProcessorSupplier(
                new JetJoinInfo(inner ? INNER : LEFT, new int[0], new int[0], null, condition),
                "map",
                rightRowProjectorSupplier
        );
        supplier.init(supplierContext);

        Processor processor = supplier.get(1).iterator().next();
        processor.init(outbox, processorContext);

        return processor;
    }
}