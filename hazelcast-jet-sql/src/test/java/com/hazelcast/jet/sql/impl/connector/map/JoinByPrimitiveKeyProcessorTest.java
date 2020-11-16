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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.IMap;
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

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class JoinByPrimitiveKeyProcessorTest {

    @Mock
    private IMap<Object, Object> map;

    @Mock
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    @Mock
    private KvRowProjector rightProjector;

    @Mock
    private ProcSupplierCtx supplierContext;

    @Mock
    private Context processorContext;

    @Mock
    private JetInstance jetInstance;

    @Mock
    private Outbox outbox;

    @Mock
    private InternalSerializationService serializationService;

    @Before
    public void setUp() {
        given(rightRowProjectorSupplier.get(any(), any())).willReturn(rightProjector);
        given(supplierContext.serializationService()).willReturn(serializationService);
        given(supplierContext.jetInstance()).willReturn(jetInstance);
        given(jetInstance.getMap(anyString())).willReturn(map);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_leftKeyIsNull_then_emptyResult() throws Exception {
        // given
        Processor processor = processor((Expression<Boolean>) ConstantExpression.create(true, BOOLEAN));

        // when
        processor.process(0, new TestInbox(singletonList(new Object[]{null})));
        processor.complete();

        // then
        verifyNoInteractions(map, rightProjector, outbox);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_rightValueIsNull_then_emptyResult() throws Exception {
        // given
        Processor processor = processor((Expression<Boolean>) ConstantExpression.create(true, BOOLEAN));

        given(map.getAsync(1)).willReturn(CompletableFuture.completedFuture(null));

        // when
        processor.process(0, new TestInbox(singletonList(new Object[]{1})));
        processor.complete();

        // then
        verifyNoInteractions(rightProjector, outbox);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_filteredOutByProjector_then_emptyResult() throws Exception {
        // given
        Processor processor = processor((Expression<Boolean>) ConstantExpression.create(true, BOOLEAN));

        given(map.getAsync(1)).willReturn(CompletableFuture.completedFuture("value"));
        given(rightProjector.project(entry(1, "value"))).willReturn(null);

        // when
        processor.process(0, new TestInbox(singletonList(new Object[]{1})));
        processor.complete();

        // then
        verifyNoInteractions(outbox);
    }

    @Test
    @SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
    public void when_projectedByProjector_then_modified() throws Exception {
        // given
        Processor processor = processor((Expression<Boolean>) ConstantExpression.create(true, BOOLEAN));

        given(map.getAsync(1)).willReturn(CompletableFuture.completedFuture("original"));
        given(rightProjector.project(entry(1, "original"))).willReturn(new Object[]{2, "modified"});

        // when
        processor.process(0, new TestInbox(singletonList(new Object[]{1})));
        processor.complete();

        // then
        verify(outbox).offer(-1, new Object[]{1, 2, "modified"});
        verifyNoMoreInteractions(outbox);
    }

    @Test
    public void when_filteredOutByCondition_then_absent() throws Exception {
        // given
        Processor processor = processor(ComparisonPredicate.create(
                ColumnExpression.create(2, VARCHAR),
                ConstantExpression.create("value-2", VARCHAR),
                ComparisonMode.EQUALS
        ));

        given(map.getAsync(1)).willReturn(CompletableFuture.completedFuture("value-1"));
        given(rightProjector.project(entry(1, "value-1"))).willReturn(new Object[]{1, "value-1"});

        // when
        processor.process(0, new TestInbox(singletonList(new Object[]{1})));
        processor.complete();

        // then
        verifyNoInteractions(outbox);
    }

    private Processor processor(Expression<Boolean> condition) throws Exception {
        ProcessorSupplier supplier = new JoinByPrimitiveKeyProcessorSupplier(
                0,
                condition,
                "map",
                rightRowProjectorSupplier
        );
        supplier.init(supplierContext);

        Processor processor = supplier.get(1).iterator().next();
        processor.init(outbox, processorContext);

        return processor;
    }
}
