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

package com.hazelcast.jet.sql.impl.connector.cdc;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.impl.CdcSource;
import com.hazelcast.jet.cdc.impl.ChangeRecordCdcSource;
import com.hazelcast.jet.cdc.impl.ConstantSequenceExtractor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.expression.MapRow;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.cdc.impl.CdcSource.SEQUENCE_EXTRACTOR_CLASS_PROPERTY;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientTimestampedSourceP;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.evaluate;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

public class CdcSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "Cdc";
    public static final String OPERATION = "__operation";

    private static final String NAME = "name";
    private static final String INCLUDE_SCHEMA_CHANGES = "include.schema.changes";
    private static final String TOMBSTONES_ON_DELETE = "tombstones.on.delete";
    private static final String DATABASE_HISTORY = "database.history";

    @Override
    public boolean isStream() {
        return true;
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull @Override
    public List<ExternalField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> userFields
    ) {
        // TODO: column property instead of predefined name?
        ExternalField operationTypeField = userFields.stream()
                                                     .filter(field -> OPERATION.equalsIgnoreCase(field.name()))
                                                     .findFirst()
                                                     .orElse(null);
        if (operationTypeField == null) {
            throw new IllegalStateException(OPERATION + " column is required");
        } else if (!VARCHAR.equals(operationTypeField.type())) {
            throw new IllegalArgumentException(OPERATION + " column must be of " + VARCHAR + " type");
        }

        return toList(userFields, ef -> new ExternalField(ef.name(), ef.type()));
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> resolvedFields
    ) {
        Properties cdcProperties = new Properties();
        cdcProperties.putAll(options);
        cdcProperties.put(NAME, tableName);
        cdcProperties.put(INCLUDE_SCHEMA_CHANGES, false);
        cdcProperties.put(TOMBSTONES_ON_DELETE, false);
        cdcProperties.put(DATABASE_HISTORY, CdcSource.DatabaseHistoryImpl.class.getName());
        cdcProperties.put(SEQUENCE_EXTRACTOR_CLASS_PROPERTY, ConstantSequenceExtractor.class.getName());

        // TODO: "database.whitelist" & "table.whitelist" in theory could be inferred <- schemaName & tableName
        return new CdcTable(this, schemaName, tableName, new ConstantTableStatistics(0),
                toList(resolvedFields, ef -> new TableField(ef.name(), ef.type(), false)), cdcProperties);
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Nullable
    @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable String timestampField,
            @Nonnull Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections
    ) {
        CdcTable table = (CdcTable) table0;

        String tableName = table.getSqlName();
        Properties properties = table.getCdcProperties();
        Vertex sourceVertex = dag.newVertex("cdc(" + tableName + ")",
                // TODO: is it ok to use CdcSource?
                convenientTimestampedSourceP(ctx -> new ChangeRecordCdcSource(properties),
                        ChangeRecordCdcSource::fillBuffer,
                        noEventTime(), // TODO: should use timestamps ?
                        ChangeRecordCdcSource::createSnapshot,
                        ChangeRecordCdcSource::restoreSnapshot,
                        ChangeRecordCdcSource::destroy,
                        0) // TODO: is it the correct value ?
        );

        FunctionEx<ChangeRecord, Object[]> mapFn = projectionFn(table, predicate, projections);
        Vertex filterProjectVertex = dag.newVertex("cdc-filter-project", mapP(mapFn));

        dag.edge(between(sourceVertex, filterProjectVertex).isolated());
        return filterProjectVertex;
    }

    private static FunctionEx<ChangeRecord, Object[]> projectionFn(
            Table table,
            Expression<Boolean> predicate,
            List<Expression<?>> projections
    ) {
        List<String> fieldNames = toList(table.getFields(), TableField::getName);
        List<QueryDataType> fieldTypes = toList(table.getFields(), TableField::getType);

        @SuppressWarnings("unchecked")
        Expression<Boolean> predicate0 = predicate != null ? predicate
                : (Expression<Boolean>) ConstantExpression.create(true, QueryDataType.BOOLEAN);

        return record -> {
            String operation;
            switch (record.operation()) {
                case SYNC:
                    operation = "s";
                    break;
                case INSERT:
                    operation = "c";
                    break;
                case UPDATE:
                    operation = "u";
                    break;
                case DELETE:
                    operation = "d";
                    break;
                case UNSPECIFIED:
                default:
                    return null;
            }

            Map<String, Object> values = record.value().toMap();
            values.put(OPERATION, operation);

            Row row = new MapRow(fieldNames, fieldTypes, values);
            if (!Boolean.TRUE.equals(evaluate(predicate0, row))) {
                return null;
            }
            Object[] result = new Object[projections.size()];
            for (int i = 0; i < projections.size(); i++) {
                result[i] = evaluate(projections.get(i), row);
            }
            return result;
        };
    }
}
