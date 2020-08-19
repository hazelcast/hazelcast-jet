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

import com.hazelcast.jet.sql.JetSqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.jet.sql.impl.schema.model.IdentifiedPerson;
import com.hazelcast.jet.sql.impl.schema.model.Person;
import com.hazelcast.sql.SqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaTest extends JetSqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getHazelcastInstance().getSql();
    }

    @Test
    public void when_tableIsDeclared_then_itIsAvailable() {
        // given
        String name = createRandomName();

        // when
        SqlResult createResult = sqlService.query(javaSerializableMapDdl(name, Integer.class, String.class));

        // then
        assertThat(createResult.isUpdateCount()).isTrue();
        assertThat(createResult.updateCount()).isEqualTo(-1);

        // when
        SqlResult updateResult = sqlService.query("SELECT __key, this FROM public." + name);

        // then
        assertThat(updateResult.isUpdateCount()).isFalse();
        assertThat(updateResult.iterator()).isExhausted();
    }

    @Test
    public void when_tableIsDeclared_then_itCanBeListed() {
        // given
        String name = createRandomName();
        String sql = "CREATE EXTERNAL TABLE \"" + name + "\" (" + System.lineSeparator()
                + "  \"__key\" INT EXTERNAL NAME \"__key\"," + System.lineSeparator()
                + "  \"this\" VARCHAR EXTERNAL NAME \"this\"" + System.lineSeparator()
                + ")" + System.lineSeparator()
                + "TYPE \"" + IMapSqlConnector.TYPE_NAME + "\"" + System.lineSeparator()
                + "OPTIONS (" + System.lineSeparator()
                + "  \"" + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'," + System.lineSeparator()
                + "  \"" + OPTION_KEY_CLASS + "\" '" + Integer.class.getName() + "'," + System.lineSeparator()
                + "  \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'," + System.lineSeparator()
                + "  \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'" + System.lineSeparator()
                + ")";
        sqlService.query(sql);

        // when
        assertRowsEventuallyAnyOrder(
                "SHOW EXTERNAL TABLES",
                singletonList(new JetSqlTestSupport.Row(name, sql))
        );
    }

    @Test
    public void when_tableIsDeclared_then_itsDefinitionHasPrecedenceOverDiscoveredOne() {
        // given
        String name = createRandomName();
        sqlService.query(javaSerializableMapDdl(name, Integer.class, Person.class));

        Map<Integer, Person> map = instance().getMap(name);
        map.put(1, new IdentifiedPerson(2, "Alice"));

        // when
        // then
        assertThatThrownBy(() -> sqlService.query("SELECT id FROM " + name))
                .isInstanceOf(SqlException.class);
    }

    @Test
    public void when_tableIsDropped_then_itIsNotAvailable() {
        // given
        String name = createRandomName();
        sqlService.query(javaSerializableMapDdl(name, Integer.class, Person.class));

        // when
        sqlService.query("DROP EXTERNAL TABLE " + name);

        // then
        assertThatThrownBy(() -> sqlService.query("SELECT * FROM public." + name))
                .isInstanceOf(SqlException.class);
    }

    @Test
    public void when_schemaNameUsed_then_rejected() {
        assertThatThrownBy(() ->
                sqlService.query(javaSerializableMapDdl("schema." + createRandomName(), Long.class, Long.class)))
                        .hasMessageContaining("Encountered \".\" at line 1, column 29");
    }

    @Test
    public void when_emptyColumnList_then_fail() {
        assertThatThrownBy(() -> sqlService.query("CREATE EXTERNAL TABLE t() TYPE t"))
                .hasMessageContaining("Encountered \")\" at line 1");
    }

    private static String createRandomName() {
        return "schema_" + randomString().replace('-', '_');
    }
}
