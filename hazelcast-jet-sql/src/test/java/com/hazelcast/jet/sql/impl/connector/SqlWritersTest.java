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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.sql.impl.connector.SqlWriters.EntryWriter;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.SqlWriters.entryWriter;
import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlWritersTest {

    @Test
    public void when_createsEntryWriterWithoutKey_then_throws() {
        // When
        // Then
        assertThatThrownBy(() -> entryWriter(
                emptyList(),
                null,
                AnObject.class.getName()
        )).isInstanceOf(JetException.class);
    }

    @Test
    public void when_createsEntryWriterWithRecordKey_then_throws() {
        // When
        // Then
        assertThatThrownBy(() -> entryWriter(
                singletonList(new ExternalField(KEY_ATTRIBUTE_NAME.value(), OBJECT)),
                AnObject.class.getName(),
                AnObject.class.getName()
        )).isInstanceOf(JetException.class);
    }

    @Test
    public void when_createsEntryWriterWithoutValue_then_throws() {
        // When
        // Then
        assertThatThrownBy(() -> entryWriter(
                emptyList(),
                AnObject.class.getName(),
                null
        )).isInstanceOf(JetException.class);
    }

    @Test
    public void when_createsEntryWriterWithRecordValue_then_throws() {
        // When
        // Then
        assertThatThrownBy(() -> entryWriter(
                singletonList(new ExternalField(THIS_ATTRIBUTE_NAME.value(), OBJECT)),
                AnObject.class.getName(),
                AnObject.class.getName()
        )).isInstanceOf(JetException.class);
    }

    @Test
    public void when_createsEntryWriterWithScalarKeyAndValue_then_succeeds() {
        // When
        EntryWriter writer = entryWriter(
                asList(
                        new ExternalField(KEY_ATTRIBUTE_NAME.value(), INT),
                        new ExternalField(THIS_ATTRIBUTE_NAME.value(), BIGINT)),
                null,
                null
        );

        // Then
        assertThat(writer).isNotNull();
    }

    @Test
    public void when_createsEntryWriterWithNonScalarKeyAndValue_then_succeeds() {
        // When
        EntryWriter writer = entryWriter(
                emptyList(),
                AnObject.class.getName(),
                AnObject.class.getName()
        );

        // Then
        assertThat(writer).isNotNull();
    }

    private static class AnObject {

        @SuppressWarnings("unused")
        private String field;
    }
}
