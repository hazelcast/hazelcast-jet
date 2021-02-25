/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class MappingDefinition {

    private final Table table;
    private final Mapping mapping;

    public MappingDefinition(Table table, Mapping mapping) {
        this.table = table;
        this.mapping = mapping;
    }

    String schema() {
        return table.getSchemaName();
    }

    String name() {
        return table.getSqlName();
    }

    String externalName() {
        return mapping.externalName();
    }

    String type() {
        return mapping.type();
    }

    List<TableField> tableFields() {
        return table.getFields();
    }

    List<MappingField> mappingFields() {
        return mapping.fields();
    }

    String options() {
        return mapping.options().entrySet().stream()
                .sorted(Entry.comparingByKey())
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
