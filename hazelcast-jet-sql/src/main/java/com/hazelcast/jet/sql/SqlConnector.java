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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public interface SqlConnector {

    boolean isStream();

    @Nullable
    default JetTable createTable(
            @Nonnull JetInstance jetInstance,
            @Nonnull String tableName,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions
    ) {
        throw new UnsupportedOperationException("Field examination not supported for " + getClass().getName());
    }

    @Nullable
    JetTable createTable(
            @Nonnull JetInstance jetInstance,
            @Nonnull String tableName,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions,
            @Nonnull List<Entry<String, QueryDataType>> fields);

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
     * @param predicate  SQL expression to filter the rows
     * @param projection list of field names to return
     */
    @Nullable
    default Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull JetTable jetTable,
            @Nullable String timestampField,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection) {
        assert !supportsFullScanReader();
        throw new UnsupportedOperationException("Full scan reader not supported for " + getClass().getName());

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
    default Tuple2<Vertex, Vertex> nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull JetTable jetTable,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection,
            @Nonnull Expression<Boolean> joinPredicate) {
        assert !supportsNestedLoopReader();
        throw new UnsupportedOperationException("Nested loop reader not supported for " + getClass().getName());
    }

    /**
     * Returns the supplier for the sink processor.
     */
    @Nullable
    default Vertex sink(
            @Nonnull DAG dag,
            @Nonnull JetTable jetTable) {
        assert !supportsSink();
        throw new UnsupportedOperationException("Sink not supported for " + getClass().getName());
    }

    default boolean supportsFullScanReader() {
        return false;
    }

    default boolean supportsNestedLoopReader() {
        return false;
    }

    default boolean supportsSink() {
        return false;
    }

    // TODO: naming ...
    default boolean supportsPlainInserts() {
        return true;
    }
}
