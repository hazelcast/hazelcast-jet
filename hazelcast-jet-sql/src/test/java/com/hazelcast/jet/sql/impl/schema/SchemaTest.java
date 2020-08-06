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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.LocalPartitionedMapConnector;
import com.hazelcast.jet.sql.impl.schema.model.IdentifiedPerson;
import com.hazelcast.jet.sql.impl.schema.model.Person;
import com.hazelcast.sql.SqlException;
import com.hazelcast.sql.SqlResult;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_VALUE_CLASS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaTest extends SqlTestSupport {

    @Test
    public void testTableCreation() {
        // given
        String name = createRandomName();

        // when
        SqlResult createResult = executeSql(
                "CREATE EXTERNAL TABLE " + name + " "
                        + "TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                        + "OPTIONS ("
                        + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_KEY_CLASS + "\" '" + Integer.class.getName() + "'"
                        + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                        + ")"
        );

        // then
        assertThat(createResult.isUpdateCount()).isTrue();
        assertThat(createResult.updateCount()).isEqualTo(-1);

        // when
        SqlResult updateResult = executeSql("SELECT __key, this FROM public." + name);

        // then
        assertThat(updateResult.isUpdateCount()).isFalse();
        assertThat(updateResult.iterator()).isExhausted();
    }

    @Test
    public void testDeclaredTablePriority() {
        // given
        String name = createRandomName();
        executeSql(
                "CREATE EXTERNAL TABLE " + name + " "
                        + "TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                        + "OPTIONS ("
                        + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_KEY_CLASS + "\" '" + Integer.class.getName() + "'"
                        + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_VALUE_CLASS + "\" '" + Person.class.getName() + "'"
                        + ")"
        );

        Map<Integer, Person> map = instance().getMap(name);
        map.put(1, new IdentifiedPerson(2, "Alice"));

        // when
        // then
        assertThatThrownBy(() -> executeSql("SELECT id FROM " + name))
                .isInstanceOf(SqlException.class);
    }

    @Test
    public void testTableRemoval() {
        // given
        String name = createRandomName();
        executeSql(
                "CREATE EXTERNAL TABLE " + name + " "
                        + "TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                        + "OPTIONS ("
                        + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_KEY_CLASS + "\" '" + Integer.class.getName() + "'"
                        + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                        + ", \"" + TO_VALUE_CLASS + "\" '" + Person.class.getName() + "'"
                        + ")"
        );

        // when
        executeSql("DROP EXTERNAL TABLE " + name);

        // then
        assertThatThrownBy(() -> executeSql("SELECT * FROM public." + name))
                .isInstanceOf(SqlException.class);
    }

    private static String createRandomName() {
        return "schema_" + randomString().replace('-', '_');
    }
}
