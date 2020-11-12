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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.Table;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * An API to bridge Jet connectors and SQL. Allows the use of a Jet
 * connector from SQL and create a mapping for remote objects available
 * through the connector.
 * <p>
 * It's currently private because we expect significant changes to it, but
 * we plan to make it part of the public Core API in the future.
 */
public interface SqlConnector {

    /**
     * The key serialization format for key-value connectors.
     */
    String OPTION_KEY_FORMAT = "keyFormat";

    /**
     * The value serialization format for key-value connectors.
     */
    String OPTION_VALUE_FORMAT = "valueFormat";

    /**
     * The key Java class, if {@value #OPTION_KEY_FORMAT} is {@value
     * JAVA_FORMAT}.
     */
    String OPTION_KEY_CLASS = "keyJavaClass";

    /**
     * The value Java class, if {@value #OPTION_VALUE_FORMAT} is {@value
     * JAVA_FORMAT}.
     */
    String OPTION_VALUE_CLASS = "valueJavaClass";

    /**
     * The key Portable factory ID, if {@value #OPTION_KEY_FORMAT} is {@value
     * PORTABLE_FORMAT}.
     */
    String OPTION_KEY_FACTORY_ID = "keyPortableFactoryId";

    /**
     * The key Portable class ID, if {@value #OPTION_KEY_FORMAT} is {@value
     * PORTABLE_FORMAT}.
     */
    String OPTION_KEY_CLASS_ID = "keyPortableClassId";

    /**
     * The key Portable class version, if {@value #OPTION_KEY_FORMAT} is
     * {@value PORTABLE_FORMAT}.
     */
    String OPTION_KEY_CLASS_VERSION = "keyPortableClassVersion";

    /**
     * The value Portable factory ID, if {@value #OPTION_VALUE_FORMAT} is
     * {@value PORTABLE_FORMAT}.
     */
    String OPTION_VALUE_FACTORY_ID = "valuePortableFactoryId";

    /**
     * The value Portable class ID, if {@value #OPTION_VALUE_FORMAT} is {@value
     * PORTABLE_FORMAT}.
     */
    String OPTION_VALUE_CLASS_ID = "valuePortableClassId";

    /**
     * The value Portable class version, if {@value #OPTION_VALUE_FORMAT} is
     * {@value PORTABLE_FORMAT}.
     */
    String OPTION_VALUE_CLASS_VERSION = "valuePortableClassVersion";

    /**
     * Value for {@value #OPTION_KEY_FORMAT} and {@value #OPTION_VALUE_FORMAT}
     * for Java serialization.
     */
    String JAVA_FORMAT = "java";

    /**
     * Value for {@value #OPTION_KEY_FORMAT} and {@value #OPTION_VALUE_FORMAT}
     * for Portable serialization.
     */
    String PORTABLE_FORMAT = "portable";

    /**
     * Value for {@value #OPTION_KEY_FORMAT} and {@value #OPTION_VALUE_FORMAT}
     * for JSON serialization.
     */
    String JSON_FORMAT = "json";

    /**
     * Value for {@value #OPTION_KEY_FORMAT} and {@value #OPTION_VALUE_FORMAT}
     * for Avro serialization.
     */
    String AVRO_FORMAT = "avro";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the accessed object name. If missing, the external table name
     * itself is used.
     */
    String OPTION_OBJECT_NAME = "objectName";

    /**
     * Return the name of the connector as seen in the {@code TYPE} clause in
     * the {@code CREATE EXTERNAL MAPPING} command.
     */
    String typeName();

    /**
     * Returns {@code true}, if {@link #fullScanReader} is an unbounded source.
     */
    boolean isStream();

    /**
     * Resolves the final field list given an initial field list and options
     * from the user. Jet calls this method when processing a CREATE MAPPING
     * statement.
     * <p>
     * The {@code userFields} can be empty, in this case the connector is
     * supposed to resolve them from a sample or from options. If it's not
     * empty, it should only validate it &mdash; it should neither add/remove
     * columns nor change the type. It can add the external name.
     * <p>
     * The returned list must not be empty. Jet stores it in the catalog, and
     * the user can see it by listing the catalog. Jet will later pass it to
     * {@link #createTable}.
     *
     * @param nodeEngine an instance of {@link NodeEngine}
     * @param options    user-provided options
     * @param userFields user-provided list of fields, possibly empty
     * @return final field list, must not be empty
     */
    @Nonnull
    List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    );

    /**
     * Creates a {@link Table} object with the given fields. Should return
     * quickly; specificially it should not attempt to connect to the remote
     * service.
     * <p>
     * Jet calls this method for each statement execution and for each mapping.
     *
     * @param nodeEngine     an instance of {@link NodeEngine}
     * @param options        connector specific options
     * @param resolvedFields list of fields as returned from {@link
     *                       #resolveAndValidateFields}
     */
    @Nonnull
    Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    );

    /**
     * Returns whether this connector supports the {@link #fullScanReader}. The
     * default implementation returns {@code false}.
     */
    default boolean supportsFullScanReader() {
        return false;
    }

    /**
     * Returns a supplier for a source vertex reading the input according to
     * the projection/predicate. The output type of the source is Object[].
     * <p>
     * The field indexes in the predicate and projection refer to the
     * zero-based indexes of the original fields of the {code table}. For
     * example, if the table has fields {@code a, b, c} and the query is:
     * <pre>{@code
     *     SELECT b FROM t WHERE c=10
     * }</pre>
     * Then the projection will be {@code {1}} and the predicate will be {@code
     * {2}=10}.
     *
     * @param table      the table object
     * @param predicate  SQL expression to filter the rows
     * @param projection the list of field names to return
     * @return The DAG Vertex handling the reading
     */
    @Nonnull
    default Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection
    ) {
        assert !supportsFullScanReader();
        throw new UnsupportedOperationException("Full scan not supported for " + typeName());

    }

    /**
     * Returns whether this connector supports the {@link #sink}. The default
     * implementation returns {@code false}.
     */
    default boolean supportsSink() {
        return false;
    }

    /**
     * Returns whether this connector supports the {@code INSERT} statement.
     * The default implementation returns {@code false}.
     */
    default boolean supportsInsert() {
        return false;
    }

    /**
     * Returns the supplier for the sink processor.
     */
    @Nonnull
    default Vertex sink(
            @Nonnull DAG dag,
            @Nonnull Table table
    ) {
        assert !supportsSink();
        throw new UnsupportedOperationException("Sink not supported for " + typeName());
    }
}
