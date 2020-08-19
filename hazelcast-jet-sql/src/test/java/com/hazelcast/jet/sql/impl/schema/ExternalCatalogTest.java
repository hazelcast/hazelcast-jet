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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExternalCatalogTest extends SimpleTestInClusterSupport {

    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory();

    private static HazelcastInstance instance;

    @BeforeClass
    public static void beforeClass() {
        instance = FACTORY.newHazelcastInstance();
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Test
    public void throws_whenTriesToCreateInvalidTable() {
        // given
        String name = "table_to_validate";
        ExternalCatalog catalog = externalCatalog();

        // when
        // then
        assertThatThrownBy(() -> catalog.createTable(
                new ExternalTable(
                        name,
                        "non.existing.connector",
                        singletonList(new ExternalField("name", INT)),
                        emptyMap()),
                true,
                true
                )
        ).isInstanceOf(QueryException.class);
        assertThat(catalog.getTables().stream().noneMatch(table -> table.getName().equals(name))).isTrue();
    }

    @Test
    public void throws_whenTriesToCreateDuplicateTable() {
        // given
        String name = "table_to_create";
        ExternalCatalog catalog = externalCatalog();
        assertThat(catalog.createTable(table(name), false, false)).isTrue();

        // when
        // then
        assertThatThrownBy(() -> catalog.createTable(table(name), false, false))
                .isInstanceOf(QueryException.class);
    }

    @Test
    public void replaces_whenTableAlreadyExists() {
        // given
        String name = "table_to_be_replaced";
        ExternalCatalog catalog = externalCatalog();
        assertThat(catalog.createTable(table(name, Integer.class, String.class), true, false)).isTrue();

        // when
        ExternalTable table = table(name, String.class, Integer.class);
        assertThat(catalog.createTable(table, true, false)).isTrue();

        // then
        assertThat(tableFields(catalog, name))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("__key", VARCHAR, "this", INT));
    }

    @Test
    public void doesNotThrow_whenTableAlreadyExists() {
        // given
        String name = "table_if_not_exists";
        ExternalCatalog catalog = externalCatalog();
        assertThat(catalog.createTable(table(name, Integer.class, String.class), false, true)).isTrue();

        // when
        ExternalTable table = table(name, String.class, Integer.class);
        assertThat(catalog.createTable(table, false, true)).isFalse();

        // then
        assertThat(tableFields(catalog, name))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("__key", INT, "this", VARCHAR));
    }

    @Test
    public void throws_whenTableDoesNotExist() {
        // given
        ExternalCatalog catalog = externalCatalog();

        // when
        // then
        assertThatThrownBy(() -> catalog.removeTable("table_to_be_removed", false))
                .isInstanceOf(QueryException.class);
    }

    @Test
    public void doesNotThrow_whenTableDoesNotExist() {
        // given
        ExternalCatalog catalog = externalCatalog();

        // when
        // then
        catalog.removeTable("table_if_exists", true);
    }

    @Test
    public void propagatesSchema() {
        // given
        String name = "table_distributed";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();

        ExternalCatalog firstInstanceCatalog = new ExternalCatalog(nodeEngine(instances[0]));
        ExternalCatalog secondInstanceCatalog = new ExternalCatalog(nodeEngine(instances[1]));

        // when table is created on one member
        firstInstanceCatalog.createTable(table(name, Integer.class, String.class), false, false);

        // then it should be available on another one
        assertTrueEventually(
                null,
                () -> assertThat(secondInstanceCatalog.getTables()).hasSize(1),
                5
        );
    }

    private static ExternalCatalog externalCatalog() {
        return new ExternalCatalog(nodeEngine(instance));
    }

    private static ExternalTable table(String name) {
        return table(name, Integer.class, String.class);
    }

    private static ExternalTable table(String name, Class<?> keyClass, Class<?> valueClass) {
        return new ExternalTable(
                name,
                IMapSqlConnector.TYPE_NAME,
                emptyList(),
                ImmutableMap.of(
                        OPTION_SERIALIZATION_KEY_FORMAT, JAVA_SERIALIZATION_FORMAT,
                        OPTION_KEY_CLASS, keyClass.getName(),
                        OPTION_SERIALIZATION_VALUE_FORMAT, JAVA_SERIALIZATION_FORMAT,
                        OPTION_VALUE_CLASS, valueClass.getName()
                )
        );
    }

    private static Map<String, QueryDataType> tableFields(ExternalCatalog catalog, String tableName) {
        return catalog.getTables().stream()
                      .filter(table -> tableName.equals(table.getName()))
                      .findFirst()
                      .map(table -> table.getFields().stream().collect(toMap(TableField::getName, TableField::getType)))
                      .orElseThrow(NoSuchElementException::new);
    }

    private static NodeEngine nodeEngine(HazelcastInstance instance) {
        return ((HazelcastInstanceProxy) instance).getOriginal().node.nodeEngine;
    }
}
