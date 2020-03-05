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

package com.hazelcast.jet.sql.imap;

import com.hazelcast.sql.impl.type.DataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;

import java.util.List;
import java.util.Map;

public class IMapTableRelDataType extends RelDataTypeImpl {

    private static final String TYPE_NAME = "IMapRow";

    public IMapTableRelDataType(Map<String, DataType> columns) {
        super(createFieldList(columns));
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("(").append(TYPE_NAME).append(getFieldNames()).append(")");
    }

    private static List<RelDataTypeField> createFieldList(Map<String, DataType> columns) {
        throw new UnsupportedOperationException("TODO");
    }
}
