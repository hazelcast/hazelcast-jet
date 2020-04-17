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
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.commons.beanutils.PropertyUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

public final class ExpressionUtil {

    private static final ExpressionEvalContext ZERO_ARGUMENTS_CONTEXT = index -> {
        throw new IndexOutOfBoundsException("" + index);
    };

    private ExpressionUtil() { }

    /**
     * Creates a function to project {@code Entry<Object, Object>} from the
     * source into {@code Object[]} that represents a row. The function
     * evaluates the predicate and returns null if it didn't pass. Then it evaluates
     * all the projections
     */
    public static FunctionEx<Entry<Object, Object>, Object[]> projectionFn(
            @Nonnull JetTable jetTable,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections
    ) {
        // convert the projection
        List<String> fieldNames0 = toList(jetTable.getFieldNames(), fieldName -> {
            // convert field name, the property path must start with "key" or "value", we're getting
            // it from a java.util.Map.Entry. Examples:
            //     "__key" -> "key"
            //     "__key.fieldA" -> "key.fieldA"
            //     "fieldB" -> "value.fieldB"
            //     "this" -> "value"
            //     "this.fieldB" -> "value.fieldB"
            if (fieldName.equals(KEY_ATTRIBUTE_NAME.value())) {
                return "key";
            } else if (fieldName.startsWith(KEY_ATTRIBUTE_NAME.value())) {
                return "key." + fieldName.substring(KEY_ATTRIBUTE_NAME.value().length());
            } else if (fieldName.equals(THIS_ATTRIBUTE_NAME.value())) {
                return "value";
            } else if (fieldName.startsWith(THIS_ATTRIBUTE_NAME.value())) {
                return "value." + fieldName.substring(THIS_ATTRIBUTE_NAME.value().length());
            } else {
                return "value." + fieldName;
            }
        });

        @SuppressWarnings("unchecked")
        Expression<Boolean> predicate0 = predicate != null ? predicate
                : (Expression<Boolean>) ConstantExpression.create(QueryDataType.BOOLEAN, true);

        return entry -> {
            EntryRow row = new EntryRow(fieldNames0, entry);
            if (!Boolean.TRUE.equals(predicate0.eval(row, ZERO_ARGUMENTS_CONTEXT))) {
                return null;
            }
            Object[] result = new Object[projections.size()];
            for (int i = 0; i < projections.size(); i++) {
                result[i] = projections.get(i).eval(row, ZERO_ARGUMENTS_CONTEXT);
            }
            return result;
        };
    }

    private static class EntryRow implements Row {

        private final List<String> fieldNames;
        private final Entry<Object, Object> entry;

        public EntryRow(List<String> fieldNames, Entry<Object, Object> entry) {
            this.fieldNames = fieldNames;
            this.entry = entry;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T get(int index) {
            try {
                return (T) PropertyUtils.getProperty(entry, fieldNames.get(index));
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw sneakyThrow(e);
            }
        }

        @Override
        public int getColumnCount() {
            return fieldNames.size();
        }
    }
}
