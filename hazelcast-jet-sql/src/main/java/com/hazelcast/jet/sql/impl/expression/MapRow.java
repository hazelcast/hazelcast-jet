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

import java.util.List;
import java.util.Map;

// TODO: use QueryTargetDescriptors instead ?
@Deprecated
public class MapRow implements Row {

    private final List<String> fieldNames;
    private final List<QueryDataType> fieldTypes;
    private final Map<String, Object> values;

    public MapRow(List<String> fieldNames, List<QueryDataType> fieldTypes, Map<String, Object> values) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.values = values;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int index) {
        Object value = values.get(fieldNames.get(index));
        // TODO: handle QueryDataTypeMismatchException and rethrow as QueryException
        return (T) fieldTypes.get(index).normalize(value); // TODO: deduplicate somehow with other types of rows ???
    }

    @Override
    public int getColumnCount() {
        return fieldTypes.size();
    }
}
