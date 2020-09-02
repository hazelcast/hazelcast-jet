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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.Table;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * TODO
 */
// TODO: merge with JetSqlConnector ???
// TODO: make this class public for user connectors
//   (TableSchemaField, Table, TableField, QueryDataType etc. need to be public then?)
public interface SqlConnector {

    // TODO do these options apply to every SQL connector? Should we move them?
    /**
     * TODO
     */
    String OPTION_SERIALIZATION_FORMAT = "serialization.format";

    /**
     * TODO
     */
    String JAVA_SERIALIZATION_FORMAT = "java";

    /**
     * TODO
     */
    String PORTABLE_SERIALIZATION_FORMAT = "portable";

    /**
     * TODO
     */
    String JSON_SERIALIZATION_FORMAT = "json";

    /**
     * TODO
     */
    String CSV_SERIALIZATION_FORMAT = "csv";

    /**
     * TODO
     */
    String AVRO_SERIALIZATION_FORMAT = "avro";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the accessed object name. If missing, the external table name
     * itself is used.
     */
    String OPTION_OBJECT_NAME = "objectName";

    /**
     * Return the name of the connector as seen in the {@code TYPE} clause in
     * the {@code CREATE EXTERNAL TABLE} command.
     */
    String typeName();

    /**
     * TODO
     */
    boolean isStream();

    /**
     *  Resolve a final field list given a field list and options from the
     *  user. The {@code userFields} can be empty, in this case the connector
     *  is supposed to resolve them. The returned list must not be empty.
     *  <p>
     *  The method is free to do any changes to the user-provided field list, but
     *  should document the behavior to the user. Generally, it should not
     *  remove columns, but might add some.
     *  <p>
     *  The returned field list will be stored in the catalog and if the user lists
     *  the catalog, they will be visible to the user. It will be later passed
     *  to {@link #createTable}.
     *
     * @param nodeEngine     an instance of {@link NodeEngine}
     * @param options        user-provided options
     * @param userFields user-provided list of fields, possibly empty
     * @return final field list, must not be empty
     */
    @Nonnull
    List<ExternalField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> userFields
    );

    /**
     * Creates a {@link Table} object with the given fields. Should not not attempt to
     * connect to the remote service.
     *
     * @param nodeEngine     an instance of {@link NodeEngine}
     * @param options        connector specific options
     * @param resolvedFields list of fields as returned from {@link
     *     #resolveAndValidateFields}
     */
    @Nonnull
    Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> resolvedFields
    );

    /**
     * TODO
     */
    default boolean supportsFullScanReader() {
        return false;
    }

    /**
     * Returns a supplier for a source vertex reading the input according to
     * the projection/predicate. The output type of the source is Object[]. If
     * timestampField is not null, the source should generate watermarks
     * according to it.
     * <p>
     * The result is:<ul>
     * <li>{@code f0}: the source vertex of the sub-DAG
     * <li>{@code f1}: the sink vertex of teh sub-DAG
     * </ul>
     * <p>
     * The field indexes in the predicate and projection both refer to indexes
     * of original fields of the jetTable. That is if the table has fields
     * {@code a, b, c} and the query is:
     *
     * <pre>{@code
     *     SELECT b FROM t WHERE c=10
     * }</pre>
     * <p>
     * Then the projection will be {@code {1}} and the predicate will be {@code
     * {2}=10}.
     *
     * @param table TODO
     * @param predicate  SQL expression to filter the rows
     * @param projection list of field names to return
     */
    @Nullable
    default Vertex fullScanReader(
            @Nonnull DAG dag,
            // TODO convert back to JetTable after we can read maps using IMDG code
            @Nonnull Table table,
            @Nullable String timestampField,
            // TODO: do we want to expose Expression to the user ?
            @Nonnull Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection) {
        assert !supportsFullScanReader();
        throw new UnsupportedOperationException("Full scan reader not supported for " + getClass().getName());

    }

    /**
     * TODO
     */
    default boolean supportsSink() {
        return false;
    }

    /**
     * TODO
     */
    // TODO: naming ...
    default boolean supportsPlainInserts() {
        return supportsSink();
    }

    /**
     * Returns the supplier for the sink processor.
     */
    @Nullable
    default Vertex sink(
            @Nonnull DAG dag,
            @Nonnull Table table) {
        assert !supportsSink();
        throw new UnsupportedOperationException("Sink not supported for " + getClass().getName());
    }
}
