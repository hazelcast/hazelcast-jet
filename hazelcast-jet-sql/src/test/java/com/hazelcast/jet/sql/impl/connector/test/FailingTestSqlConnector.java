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

package com.hazelcast.jet.sql.impl.connector.test;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.Collections.singletonList;

/**
 * A SQL source that fails immediately.
 */
public class FailingTestSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "FailingSource";

    private static final List<ExternalField> FIELD_LIST = singletonList(new ExternalField("v", QueryDataType.BIGINT));

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Nonnull @Override
    public List<ExternalField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> userFields
    ) {
        if (userFields.size() > 0) {
            throw QueryException.error("Don't specify external fields, they are fixed");
        }
        return FIELD_LIST;
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> resolvedFields
    ) {
        return new JetTable(
                this,
                toList(resolvedFields, ef -> new TableField(ef.name(), ef.type(), false)),
                schemaName, tableName, new ConstantTableStatistics(0)
        );
    }

    @Nullable @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table,
            @Nullable String timestampField,
            @Nonnull Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection
    ) {
        return dag.newVertex("failingSource", FailingP::new);
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    private static final class FailingP extends AbstractProcessor {
        @Override
        public boolean complete() {
            throw new RuntimeException("mock failure");
        }
    }
}
