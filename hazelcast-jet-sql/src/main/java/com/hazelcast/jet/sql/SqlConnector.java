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
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.schema.JetTable;
import com.hazelcast.sql.impl.type.DataType;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public interface SqlConnector {

    boolean isStream();

    @Nullable
    default JetTable createTable(
            @Nonnull String tableName,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions
    ) {
        throw new UnsupportedOperationException("Column examination not supported for " + getClass().getName());
    }

    @Nullable
    JetTable createTable(
            @Nonnull String tableName,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions,
            @Nonnull Map<String, DataType> columns
    );

    /**
     * Returns a supplier for a source vertex reading the input according to
     * the projection/predicate. The output type of the source is Object[]. If
     * timestampColumn is not null, the source should generate watermarks
     * according to it.
     *
     * @param predicate  SQL expression to filter the rows
     * @param projection list of column names to return
     */
    @Nullable
    Tuple2<Vertex, Vertex> fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions,
            @Nullable String timestampColumn,
            @Nonnull RexNode predicate,
            @Nonnull List<String> projection
    );

    /**
     * Returns a supplier for a reader that reads a set of records for the
     * given parameters it receives on the input.
     * <p>
     * It's expected to return null if {@link #isStream()} returns {@code
     * true}.
     *
     * @param predicateWithParams A predicate with positional parameters which
     *                           will be provided at runtime as the input to
     *                           the returned function.
     * @param projection list of column names to return
     */
    @Nullable
    Tuple2<Vertex, Vertex> nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions,
            @Nonnull RexNode predicateWithParams,
            @Nonnull List<String> projection);

    /**
     * Returns the supplier for the sink processor.
     *
     * @param columns list of column names given to the sink
     */
    @Nullable
    Tuple2<Vertex, Vertex> sink(
            @Nonnull DAG dag,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions,
            @Nonnull List<String> columns
    );

    boolean supportsFullScanReader();
    boolean supportsNestedLoopReader();
    boolean supportsSink();
}
