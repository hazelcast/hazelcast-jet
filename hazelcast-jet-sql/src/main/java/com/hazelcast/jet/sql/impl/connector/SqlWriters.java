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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import org.apache.calcite.util.Pair;
import org.apache.commons.beanutils.PropertyUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.ReflectionUtils.fieldsOf;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_VALUE_CLASS;
import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class SqlWriters {

    public static EntryWriter entryWriter(@Nonnull List<Entry<String, QueryDataType>> fields,
                                          @Nullable String keyClassName,
                                          @Nullable String valueClassName) {
        validateEntrySchema(fields, keyClassName, valueClassName);
        return createEntryWriter(fields, keyClassName, valueClassName);
    }

    private static void validateEntrySchema(List<Entry<String, QueryDataType>> fields,
                                            String keyClassName,
                                            String valueClassName) {
        List<String> schemaFieldNames = Pair.left(fields);
        int schemaKeyIndex = schemaFieldNames.indexOf(KEY_ATTRIBUTE_NAME.value());
        int schemaValueIndex = schemaFieldNames.indexOf(THIS_ATTRIBUTE_NAME.value());

        if (keyClassName == null && schemaKeyIndex == -1) {
            throw new JetException("You need to either specify " + TO_KEY_CLASS
                    + " in table options or declare the " + KEY_ATTRIBUTE_NAME.value() + " column");
        }
        if (keyClassName != null && schemaKeyIndex >= 0) {
            throw new JetException("Records as keys are not supported");
        }
        if (valueClassName == null && schemaValueIndex == -1) {
            throw new JetException("You need to either specify " + TO_VALUE_CLASS
                    + " in table options or declare the " + THIS_ATTRIBUTE_NAME.value() + " column");
        }
        if (valueClassName != null && schemaValueIndex >= 0) {
            throw new JetException("Records as values are not supported");
        }

        Set<String> keyFieldNames = keyClassName == null ? emptySet() : fieldsOf(keyClassName).map(Field::getName).collect(toSet());
        Set<String> valueFieldNames = valueClassName == null ? emptySet() : fieldsOf(valueClassName).map(Field::getName).collect(toSet());
        for (int index = 0; index < schemaFieldNames.size(); index++) {
            if (index != schemaKeyIndex && index != schemaValueIndex) {
                String schemaFieldName = schemaFieldNames.get(index);
                boolean keyContainsSchemaField = keyFieldNames.contains(schemaFieldName);
                boolean valueContainsSchemaField = valueFieldNames.contains(schemaFieldName);
                if (!valueContainsSchemaField && !keyContainsSchemaField) {
                    throw new JetException("Missing mapping for '" + schemaFieldName + "'");
                }
            }
        }
    }

    private static EntryWriter createEntryWriter(List<Entry<String, QueryDataType>> fields,
                                                 String keyClassName,
                                                 String valueClassName) {
        List<String> schemaFieldNames = toList(fields, Entry::getKey);
        int schemaKeyIndex = schemaFieldNames.indexOf(KEY_ATTRIBUTE_NAME.value());
        int schemaValueIndex = schemaFieldNames.indexOf(THIS_ATTRIBUTE_NAME.value());
        Set<String> valueFieldNames = valueClassName == null ? emptySet() :
                fieldsOf(valueClassName).map(Field::getName).collect(toSet());

        BitSet keyIndices = new BitSet(fields.size());
        for (int index = 0; index < schemaFieldNames.size(); index++) {
            if (index != schemaKeyIndex && index != schemaValueIndex) {
                String schemaFieldName = schemaFieldNames.get(index);
                if (!valueFieldNames.contains(schemaFieldName)) {
                    keyIndices.set(index);
                }
            }
        }

        return new EntryWriter(schemaKeyIndex, keyClassName, schemaValueIndex, valueClassName, fields, keyIndices);
    }

    private static Object convert(Object value, QueryDataType type) {
        Converter typeConverter = type.getConverter();
        if (value == null || typeConverter.getValueClass() == value.getClass()) {
            return value;
        }
        Converter converter = Converters.getConverter(value.getClass());
        return typeConverter.convertToSelf(converter, value);
    }

    public static class EntryWriter implements BiFunctionEx<Context, Object[], Entry<Object, Object>> {

        private final int wholeKeyIndex;
        private final String keyClassName;

        private final int wholeValueIndex;
        private final String valueClassName;

        private final List<Entry<String, QueryDataType>> fields;
        private final BitSet keyIndices;

        public EntryWriter(int wholeKeyIndex, String keyClassName,
                           int wholeValueIndex, String valueClassName,
                           List<Entry<String, QueryDataType>> fields, BitSet keyIndices) {
            this.wholeKeyIndex = wholeKeyIndex;
            this.keyClassName = keyClassName;
            this.wholeValueIndex = wholeValueIndex;
            this.valueClassName = valueClassName;
            this.fields = fields;
            this.keyIndices = keyIndices;
        }

        @Override
        public Entry<Object, Object> applyEx(Context context, Object[] row) throws Exception {
            assert row.length == fields.size();

            Object key = wholeKeyIndex >= 0 ? convert(row[wholeKeyIndex], fields.get(wholeKeyIndex).getValue()) :
                    newInstance(keyClassName);
            Object value = wholeValueIndex >= 0 ? convert(row[wholeValueIndex], fields.get(wholeValueIndex).getValue()) :
                    newInstance(valueClassName);
            for (int index = 0; index < row.length; index++) {
                if (index != wholeKeyIndex && index != wholeValueIndex) {
                    Entry<String, QueryDataType> schemaEntry = fields.get(index);
                    Object target = keyIndices.get(index) ? key : value;
                    PropertyUtils.setProperty(target, schemaEntry.getKey(), convert(row[index], schemaEntry.getValue()));
                }
            }
            return entry(key, value);
        }
    }
}
