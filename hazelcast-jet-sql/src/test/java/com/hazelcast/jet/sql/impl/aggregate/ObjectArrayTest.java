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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectArrayTest {

    @Test
    public void test_equals() {
        ObjectArray arrayOne = new ObjectArray(new Object[]{1, null, "s", 2L});
        ObjectArray arrayTwo = new ObjectArray(new Object[]{1, null, "s", 2L});

        assertThat(arrayOne.equals(arrayTwo)).isTrue();
    }

    @Test
    public void test_hashCode() {
        ObjectArray arrayOne = new ObjectArray(new Object[]{1, null, "s", 2L});
        ObjectArray arrayTwo = new ObjectArray(new Object[]{1, null, "s", 2L});

        assertThat(arrayOne.hashCode()).isEqualTo(arrayTwo.hashCode());
    }

    @Test
    public void test_serialization() {
        ObjectArray original = new ObjectArray(new Object[]{1, null, "s", 2L});

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ObjectArray serialized = ss.toObject(ss.toData(original));

        // then
        assertThat(serialized).isEqualTo(original);
    }
}
