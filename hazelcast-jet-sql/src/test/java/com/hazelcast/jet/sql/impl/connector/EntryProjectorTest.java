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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class EntryProjectorTest {

    @Mock
    private UpsertTarget keyTarget;

    @Mock
    private UpsertInjector keyInjector;

    @Mock
    private UpsertTarget valueTarget;

    @Mock
    private UpsertInjector valueInjector;

    @Test
    public void test_project() {
        given(keyTarget.createInjector(null)).willReturn(keyInjector);
        given(keyTarget.conclude()).willReturn(2);

        given(valueTarget.createInjector(null)).willReturn(valueInjector);
        given(valueTarget.conclude()).willReturn("4");

        EntryProjector projector = new EntryProjector(
                keyTarget,
                valueTarget,
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.VARCHAR},
                new Boolean[]{false, false}
        );

        Entry<Object, Object> entry = projector.project(new Object[]{1, "2"});

        assertThat(entry.getKey()).isEqualTo(2);
        assertThat(entry.getValue()).isEqualTo("4");
        verify(keyTarget).init();
        verify(valueTarget).init();
        verify(keyInjector).set(1);
        verify(valueInjector).set("2");
    }

    @Test
    public void test_projectWithHiddenFields() {
        given(keyTarget.createInjector(null)).willReturn(keyInjector);
        given(keyTarget.conclude()).willReturn(2);

        given(valueTarget.createInjector("field2")).willReturn(valueInjector);
        given(valueTarget.conclude()).willReturn("4");

        EntryProjector projector = new EntryProjector(
                keyTarget,
                valueTarget,
                new QueryPath[]{
                        QueryPath.KEY_PATH,
                        QueryPath.create(QueryPath.VALUE_PREFIX + "field1"),
                        QueryPath.create(QueryPath.VALUE_PREFIX + "field2")
                },
                new QueryDataType[]{QueryDataType.INT, QueryDataType.VARCHAR, QueryDataType.VARCHAR},
                new Boolean[]{false, true, false}
        );

        Entry<Object, Object> entry = projector.project(new Object[]{1, null, "2"});

        assertThat(entry.getKey()).isEqualTo(2);
        assertThat(entry.getValue()).isEqualTo("4");
        verify(keyTarget).init();
        verify(valueTarget).init();
        verify(keyInjector).set(1);
        verify(valueInjector).set("2");
        verify(valueTarget, never()).createInjector("field1");
    }
}
