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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toList;

public interface JetTableFunction extends TableFunction {

    default HazelcastTable toTable(FunctionRelDataType rowType) {
        Map<String, String> options = rowType.options();

        List<MappingField> fields = toList(
                rowType.getFieldList(),
                field -> new MappingField(field.getName(), SqlToQueryType.map(field.getType().getSqlTypeName()))
        );

        return table(options, fields);
    }

    HazelcastTable table(Map<String, String> options, List<MappingField> fields);

    class FunctionRelDataType implements RelDataType {

        private final RelDataType delegate;
        private final Map<String, String> options;

        public FunctionRelDataType(RelDataType delegate, Map<String, String> options) {
            this.delegate = delegate;
            this.options = options;
        }

        protected Map<String, String> options() {
            return options;
        }

        @Override
        public boolean isStruct() {
            return delegate.isStruct();
        }

        @Override
        public List<RelDataTypeField> getFieldList() {
            return delegate.getFieldList();
        }

        @Override
        public List<String> getFieldNames() {
            return delegate.getFieldNames();
        }

        @Override
        public int getFieldCount() {
            return delegate.getFieldCount();
        }

        @Override
        public StructKind getStructKind() {
            return delegate.getStructKind();
        }

        @Override
        public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
            return delegate.getField(fieldName, caseSensitive, elideRecord);
        }

        @Override
        public boolean isNullable() {
            return delegate.isNullable();
        }

        @Override
        public RelDataType getComponentType() {
            return delegate.getComponentType();
        }

        @Override
        public RelDataType getKeyType() {
            return delegate.getKeyType();
        }

        @Override
        public RelDataType getValueType() {
            return delegate.getValueType();
        }

        @Override
        public Charset getCharset() {
            return delegate.getCharset();
        }

        @Override
        public SqlCollation getCollation() {
            return delegate.getCollation();
        }

        @Override
        public SqlIntervalQualifier getIntervalQualifier() {
            return delegate.getIntervalQualifier();
        }

        @Override
        public int getPrecision() {
            return delegate.getPrecision();
        }

        @Override
        public int getScale() {
            return delegate.getScale();
        }

        @Override
        public SqlTypeName getSqlTypeName() {
            return delegate.getSqlTypeName();
        }

        @Override
        public SqlIdentifier getSqlIdentifier() {
            return delegate.getSqlIdentifier();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public String getFullTypeString() {
            return delegate.getFullTypeString();
        }

        @Override
        public RelDataTypeFamily getFamily() {
            return delegate.getFamily();
        }

        @Override
        public RelDataTypePrecedenceList getPrecedenceList() {
            return delegate.getPrecedenceList();
        }

        @Override
        public RelDataTypeComparability getComparability() {
            return delegate.getComparability();
        }

        @Override
        public boolean isDynamicStruct() {
            return delegate.isDynamicStruct();
        }
    }
}
