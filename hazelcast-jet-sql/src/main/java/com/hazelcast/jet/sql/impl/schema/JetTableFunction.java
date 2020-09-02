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
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TableFunction;

import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;

public interface JetTableFunction extends TableFunction {

    default PlanNodeFieldTypeProvider parametersSchema(RelDataTypeFactory typeFactory) {
        List<QueryDataType> types = toList(
                getParameters(),
                parameter -> SqlToQueryType.map(parameter.getType(typeFactory).getSqlTypeName())
        );
        return new PlanNodeSchema(types);
    }

    HazelcastTable table(List<Object> arguments, RelDataType rowType);
}
