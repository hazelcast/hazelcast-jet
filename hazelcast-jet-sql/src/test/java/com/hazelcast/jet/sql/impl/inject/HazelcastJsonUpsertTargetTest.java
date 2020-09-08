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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.core.HazelcastJsonValue;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HazelcastJsonUpsertTargetTest {

    @Test
    public void test_set() {
        UpsertTarget target = new HazelcastJsonUpsertTarget();
        UpsertInjector booleanInjector = target.createInjector("boolean");
        UpsertInjector byteInjector = target.createInjector("byte");
        UpsertInjector shortInjector = target.createInjector("short");
        UpsertInjector intInjector = target.createInjector("int");
        UpsertInjector longInjector = target.createInjector("long");
        UpsertInjector floatInjector = target.createInjector("float");
        UpsertInjector doubleInjector = target.createInjector("double");
        UpsertInjector stringInjector = target.createInjector("string");
        UpsertInjector nullInjector = target.createInjector("null");

        target.init();
        booleanInjector.set(true);
        byteInjector.set((byte) 1);
        shortInjector.set((short) 2);
        intInjector.set(3);
        longInjector.set(4L);
        floatInjector.set(5.1F);
        doubleInjector.set(5.2D);
        stringInjector.set("6");
        nullInjector.set(null);
        Object value = target.conclude();

        assertThat(value).isEqualTo(new HazelcastJsonValue(
                "{"
                        + "\"boolean\":true"
                        + ",\"byte\":1"
                        + ",\"short\":2"
                        + ",\"int\":3"
                        + ",\"long\":4"
                        + ",\"float\":5.1"
                        + ",\"double\":5.2"
                        + ",\"string\":\"6\""
                        + ",\"null\":null"
                        + "}"
        ));
    }
}
