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

import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.commons.beanutils.PropertyUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

// TODO: use QueryTargetDescriptors instead ?
@Deprecated
class EntryRow implements Row {

    private final List<String> fieldNames;
    private final List<QueryDataType> fieldTypes;
    private final Entry<Object, Object> entry;

    EntryRow(
            List<String> fieldNames,
            List<QueryDataType> fieldTypes,
            Entry<Object, Object> entry
    ) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.entry = entry;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int index) {
        try {
            Object value = PropertyUtils.getProperty(entry, fieldNames.get(index));
            // TODO: handle QueryDataTypeMismatchException and rethrow as QueryException
            return (T) fieldTypes.get(index).normalize(value); // TODO: deduplicate somehow with other types of rows ???
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public int getColumnCount() {
        return fieldTypes.size();
    }
}
