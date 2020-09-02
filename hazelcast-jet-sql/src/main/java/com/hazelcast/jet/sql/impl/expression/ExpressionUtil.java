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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.Util.toList;

public final class ExpressionUtil {

    public static final ExpressionEvalContext ZERO_ARGUMENTS_CONTEXT = index -> {
        throw new IndexOutOfBoundsException("" + index);
    };

    private ExpressionUtil() {
    }

    public static FunctionEx<Object[], Object[]> filterFn(
            Expression<Boolean> predicate
    ) {
        return values -> {
            Row row = new HeapRow(values);
            if (!Boolean.TRUE.equals(evaluate(predicate, row))) {
                return null;
            }
            return values;
        };
    }

    public static FunctionEx<Object[], Object[]> projectionFn(
            List<Expression<?>> projections
    ) {
        return values -> {
            Row row = new HeapRow(values);
            Object[] result = new Object[projections.size()];
            for (int i = 0; i < projections.size(); i++) {
                result[i] = evaluate(projections.get(i), row);
            }
            return result;
        };
    }

    /**
     * Creates a function to project {@code Entry<Object, Object>} from the
     * source into {@code Object[]} that represents a row. The function
     * evaluates the predicate and returns null if it didn't pass. Then it evaluates
     * all the projections
     */
    @Deprecated // TODO: use descriptors/targets
    public static FunctionEx<Entry<Object, Object>, Object[]> projectionFn(
            Table table,
            Expression<Boolean> predicate,
            List<Expression<?>> projections
    ) {
        List<String> fieldNames = table.getFields().stream()
                .map(field -> {
                    // TODO: get rid of casting ???
                    QueryPath path = ((MapTableField) field).getPath();
                    if (path.isKey()) {
                        return path.getPath() == null ? "key" : "key." + path.getPath();
                    } else {
                        return path.getPath() == null ? "value" : "value." + path.getPath();
                    }
                }).collect(Collectors.toList());
        List<QueryDataType> fieldTypes = toList(table.getFields(), TableField::getType);

        @SuppressWarnings("unchecked")
        Expression<Boolean> predicate0 = predicate != null ? predicate
                : (Expression<Boolean>) ConstantExpression.create(true, QueryDataType.BOOLEAN);

        return entry -> {
            // TODO: use something like MapScanRow ???
            Row row = new EntryRow(fieldNames, fieldTypes, entry);
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

    public static <T> T evaluate(Expression<T> expression, Row row) {
        return expression.eval(row, ZERO_ARGUMENTS_CONTEXT);
    }
}
