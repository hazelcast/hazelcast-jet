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

    String TO_SERIALIZATION_FORMAT = "serialization.format";

    String JAVA_SERIALIZATION_FORMAT = "java";
    String PORTABLE_SERIALIZATION_FORMAT = "portable";
    String JSON_SERIALIZATION_FORMAT = "json";
    String CSV_SERIALIZATION_FORMAT = "csv";
    String AVRO_SERIALIZATION_FORMAT = "avro";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the accessed object name. If missing, the external table name
     * itself is used.
     */
    String TO_OBJECT_NAME = "objectName";

    /**
     * Return the name of the connector as seen in the {@code TYPE} clause in
     * the {@code CREATE EXTERNAL TABLE} command.
     */
    String typeName();

    /**
     * @return
     */
    boolean isStream();

    /**
     * Creates a Table object with the given fields. Will not attempt to
     * connect to the remote service.
     *
     * @param externalFields optional list of fields. If {@code null},
     *                       an attempt to resolve them automatically will be made. If not
     *                       successful, an exception will be thrown. An empty list is
     *                       valid, it means there are zero columns in the table: you can
     *                       still query hidden fields or count the records. No validation
     *                       is performed: if fields are given, no connection to remote
     *                       source is made.
     */
    @Nonnull
    Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> externalFields
    );

    /**
     * @return
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
     * @param table
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
     * @return
     */
    default boolean supportsNestedLoopReader() {
        return false;
    }

    /**
     * Returns a supplier for a reader that reads a set of records for the
     * given parameters it receives on the input.
     * <p>
     * It's expected to return null if {@link #isStream()} returns {@code
     * true}.
     *
     * @param predicate     SQL expression to filter the rows
     * @param projection    list of field names to return
     * @param joinPredicate A joinPredicate with positional parameters which
     *                      will be provided at runtime as the input to
     *                      the returned function.
     */
    @Nullable
    default Vertex nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull Table table,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection,
            @Nonnull Expression<Boolean> joinPredicate) {
        assert !supportsNestedLoopReader();
        throw new UnsupportedOperationException("Nested loop reader not supported for " + getClass().getName());
    }

    /**
     * @return
     */
    default boolean supportsSink() {
        return false;
    }

    /**
     * @return
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
