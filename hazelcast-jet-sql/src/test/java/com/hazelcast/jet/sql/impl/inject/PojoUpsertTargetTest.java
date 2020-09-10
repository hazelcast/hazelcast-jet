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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.sql.impl.QueryException;
import org.junit.Test;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PojoUpsertTargetTest {

    @Test
    public void test_set() {
        UpsertTarget target = new PojoUpsertTarget(
                Pojo.class.getName(),
                ImmutableMap.of("intField", int.class.getName(), "stringField", String.class.getName())
        );
        UpsertInjector intFieldInjector = target.createInjector("intField");
        UpsertInjector stringFieldInjector = target.createInjector("stringField");

        target.init();
        intFieldInjector.set(1);
        stringFieldInjector.set("2");
        Object pojo = target.conclude();

        assertThat(pojo).isEqualTo(new Pojo(1, "2"));
    }

    @Test
    public void when_injectNonExistingPropertyValue_then_throws() {
        UpsertTarget target = new PojoUpsertTarget(
                Object.class.getName(),
                ImmutableMap.of("field", int.class.getName())
        );
        UpsertInjector injector = target.createInjector("field");

        target.init();
        assertThatThrownBy(() -> injector.set(1)).isInstanceOf(QueryException.class);
    }

    @Test
    public void when_injectNonExistingPropertyNullValue_then_succeeds() {
        UpsertTarget target = new PojoUpsertTarget(
                Object.class.getName(),
                ImmutableMap.of("field", int.class.getName())
        );
        UpsertInjector injector = target.createInjector("field");

        target.init();
        injector.set(null);
        Object pojo = target.conclude();

        assertThat(pojo).isNotNull();
    }

    @SuppressWarnings("unused")
    private static final class Pojo {

        public String stringField;
        private int intField;

        Pojo() {
        }

        private Pojo(int intField, String stringField) {
            this.intField = intField;
            this.stringField = stringField;
        }

        public int getIntField() {
            return intField;
        }

        public void setIntField(int intField) {
            this.intField = intField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Pojo pojo = (Pojo) o;
            return intField == pojo.intField &&
                    Objects.equals(stringField, pojo.stringField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intField, stringField);
        }
    }
}
