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

package com.hazelcast.jet.sql.impl.connector.file;

import com.fasterxml.jackson.jr.stree.JrsArray;
import com.fasterxml.jackson.jr.stree.JrsBoolean;
import com.fasterxml.jackson.jr.stree.JrsNull;
import com.fasterxml.jackson.jr.stree.JrsNumber;
import com.fasterxml.jackson.jr.stree.JrsObject;
import com.fasterxml.jackson.jr.stree.JrsString;
import com.fasterxml.jackson.jr.stree.JrsValue;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

public class JsonResolverTest {

    @Test
    public void test_resolveFields() {
        // given
        JrsObject object = new JrsObject(new LinkedHashMap<String, JrsValue>() {
            {
                put("boolean", JrsBoolean.TRUE);
                put("number", new JrsNumber(1));
                put("string", new JrsString("string"));
                put("object", new JrsObject(emptyMap()));
                put("array", new JrsArray(emptyList()));
                put("nullValue", null);
                put("null", new JrsNull());
            }
        });

        // when
        List<MappingField> fields = JsonResolver.resolveFields(object);

        // then
        assertThat(fields).hasSize(7);
        assertThat(fields.get(0)).isEqualTo(new MappingField("boolean", QueryDataType.BOOLEAN));
        assertThat(fields.get(1)).isEqualTo(new MappingField("number", QueryDataType.DOUBLE));
        assertThat(fields.get(2)).isEqualTo(new MappingField("string", QueryDataType.VARCHAR));
        assertThat(fields.get(3)).isEqualTo(new MappingField("object", QueryDataType.OBJECT));
        assertThat(fields.get(4)).isEqualTo(new MappingField("array", QueryDataType.OBJECT));
        assertThat(fields.get(5)).isEqualTo(new MappingField("nullValue", QueryDataType.OBJECT));
        assertThat(fields.get(6)).isEqualTo(new MappingField("null", QueryDataType.OBJECT));
    }
}
