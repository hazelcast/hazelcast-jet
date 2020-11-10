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

import com.google.common.collect.ImmutableSet;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.sql.impl.JoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.extract.QueryPath;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class JoinByPredicateProcessorFactoryTest {

    @Mock
    private IMap<Object, Object> map;

    @Mock
    private KvRowProjector rightProjector;

    @Mock
    private Outbox outbox;

    @Mock
    private Context context;

    @Before
    public void setUp() {
        given(outbox.offer(anyInt(), any())).willReturn(true);
    }

    @Test
    @SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
    public void when_filteredOutByProjector_then_absent() throws Exception {
        // given
        Processor processor = processor((Expression<Boolean>) ConstantExpression.create(true, BOOLEAN));
        processor.init(outbox, context);

        given(map.entrySet(any(Predicate.class))).willReturn(ImmutableSet.of(entry(1, "value-1"), entry(2, "value-2")));
        given(rightProjector.project(entry(1, "value-1"))).willReturn(null);
        given(rightProjector.project(entry(2, "value-2"))).willReturn(new Object[]{2, "value-2"});

        // when
        processor.process(0, new TestInbox(asList(new Object[]{1}, new Object[]{2})));
        processor.complete();

        // then
        verify(outbox).offer(-1, new Object[]{1, 2, "value-2"});
        verify(outbox).offer(-1, new Object[]{2, 2, "value-2"});
        verifyNoMoreInteractions(outbox);
    }

    @Test
    @SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
    public void when_projectedByProjector_then_modified() throws Exception {
        // given
        Processor processor = processor((Expression<Boolean>) ConstantExpression.create(true, BOOLEAN));
        processor.init(outbox, context);

        given(map.entrySet(any(Predicate.class))).willReturn(ImmutableSet.of(entry(1, "value-1")));
        given(rightProjector.project(entry(1, "value-1"))).willReturn(new Object[]{2, "modified"});

        // when
        processor.process(0, new TestInbox(singletonList(new Object[]{1})));
        processor.complete();

        // then
        verify(outbox).offer(-1, new Object[]{1, 2, "modified"});
        verifyNoMoreInteractions(outbox);
    }

    @Test
    @SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
    public void when_filteredOutByCondition_then_absent() throws Exception {
        // given
        Processor processor = processor(ComparisonPredicate.create(
                ColumnExpression.create(0, INT),
                ColumnExpression.create(1, INT),
                ComparisonMode.EQUALS
        ));
        processor.init(outbox, context);

        given(map.entrySet(any(Predicate.class))).willReturn(ImmutableSet.of(entry(1, "value-1"), entry(2, "value-2")));
        given(rightProjector.project(entry(1, "value-1"))).willReturn(new Object[]{1, "value-1"});
        given(rightProjector.project(entry(2, "value-2"))).willReturn(new Object[]{2, "value-2"});

        // when
        processor.process(0, new TestInbox(asList(new Object[]{0}, new Object[]{1})));
        processor.complete();

        // then
        verify(outbox).offer(-1, new Object[]{1, 1, "value-1"});
        verifyNoMoreInteractions(outbox);
    }

    private Processor processor(
            Expression<Boolean> nonEquiCondition
    ) {
        return JoinByPredicateProcessorFactory.INSTANCE.create(
                null,
                map,
                new QueryPath[]{QueryPath.KEY_PATH},
                () -> rightProjector,
                new JoinInfo(new int[]{0}, new int[]{0}, nonEquiCondition, null)
        );
    }
}
