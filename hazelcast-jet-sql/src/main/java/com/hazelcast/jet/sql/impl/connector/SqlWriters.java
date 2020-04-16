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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.util.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.beans.PropertyDescriptor;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.ReflectionUtils.loadClass;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.type.converter.ToConverters.getToConverter;
import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.beanutils.PropertyUtils.getPropertyDescriptors;
import static org.apache.commons.beanutils.PropertyUtils.setProperty;

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
        List<String> fieldNames = Pair.left(fields);
        int keyIndex = fieldNames.indexOf(KEY_ATTRIBUTE_NAME.value());
        int valueIndex = fieldNames.indexOf(THIS_ATTRIBUTE_NAME.value());

        if ((keyClassName == null && keyIndex == -1) || (keyClassName != null && keyIndex >= 0)) {
            throw new JetException("You need to either specify " + TO_KEY_CLASS + " in table options or declare the " +
                    KEY_ATTRIBUTE_NAME.value() + " column but not both");
        }
        if ((valueClassName == null && valueIndex == -1) || (valueClassName != null && valueIndex >= 0)) {
            throw new JetException("You need to either specify " + TO_VALUE_CLASS + " in table options or declare the " +
                    THIS_ATTRIBUTE_NAME.value() + " column but not both");
        }
    }

    private static EntryWriter createEntryWriter(List<Entry<String, QueryDataType>> fields,
                                                 String keyClassName,
                                                 String valueClassName) {
        List<String> fieldNames = Pair.left(fields);
        int keyIndex = fieldNames.indexOf(KEY_ATTRIBUTE_NAME.value());
        int valueIndex = fieldNames.indexOf(THIS_ATTRIBUTE_NAME.value());

        Set<String> keyProperties = keyClassName == null ? emptySet() : propertiesOf(keyClassName);
        Set<String> valueProperties = valueClassName == null ? emptySet() : propertiesOf(valueClassName);

        BitSet keyIndices = new BitSet(fields.size());
        for (int index = 0; index < fields.size(); index++) {
            if (index != keyIndex && index != valueIndex) {
                String fieldName = fields.get(index).getKey();
                if (!valueProperties.contains(fieldName) && keyProperties.contains(fieldName)) {
                    keyIndices.set(index);
                }
            }
        }

        return new EntryWriter(keyIndex, keyClassName, valueIndex, valueClassName, keyIndices, fields);
    }

    private static Set<String> propertiesOf(String className) {
        return stream(getPropertyDescriptors(loadClass(className)))
                .map(PropertyDescriptor::getName)
                .collect(toSet());
    }

    public static class EntryWriter implements FunctionEx<Object[], Entry<Object, Object>> {

        private final int wholeKeyIndex;
        private final String keyClassName;

        private final int wholeValueIndex;
        private final String valueClassName;

        private final BitSet keyIndices;
        private final List<Entry<String, QueryDataType>> fields;

        public EntryWriter(int wholeKeyIndex, String keyClassName,
                           int wholeValueIndex, String valueClassName,
                           BitSet keyIndices, List<Entry<String, QueryDataType>> fields) {
            this.wholeKeyIndex = wholeKeyIndex;
            this.keyClassName = keyClassName;

            this.wholeValueIndex = wholeValueIndex;
            this.valueClassName = valueClassName;

            this.keyIndices = keyIndices;
            this.fields = fields;
        }

        @Override
        public Entry<Object, Object> applyEx(Object[] row) throws Exception {
            assert row.length == fields.size();

            Object key = wholeKeyIndex >= 0 ?
                    getToConverter(fields.get(wholeKeyIndex).getValue()).convert(row[wholeKeyIndex]) :
                    newInstance(keyClassName);
            Object value = wholeValueIndex >= 0 ?
                    getToConverter(fields.get(wholeValueIndex).getValue()).convert(row[wholeValueIndex]) :
                    newInstance(valueClassName);

            for (int index = 0; index < row.length; index++) {
                Object rowValue = row[index];
                if (index != wholeKeyIndex && index != wholeValueIndex && rowValue != null) {
                    Object target = keyIndices.get(index) ? key : value;
                    Object converted = getToConverter(fields.get(index).getValue()).convert(rowValue);
                    setProperty(target, fields.get(index).getKey(), converted);
                }
            }
            return entry(key, value);
        }
    }
}
