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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

final class JoinProcessors {

    private JoinProcessors() {
    }

    static ProcessorSupplier joiner(
            JetJoinInfo joinInfo,
            PartitionedMapTable table,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        String name = table.getMapName();
        List<TableField> fields = table.getFields();
        QueryPath[] paths = fields.stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
        QueryDataType[] types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
        QueryTargetDescriptor keyDescriptor = table.getKeyDescriptor();
        QueryTargetDescriptor valueDescriptor = table.getValueDescriptor();

        KvRowProjector.Supplier rightRowProjectorSupplier =
                KvRowProjector.supplier(paths, types, keyDescriptor, valueDescriptor, predicate, projection);

        if (isEquiJoinByPrimitiveKey(joinInfo, fields)) {
            return new JoinByPrimitiveKeyProcessorSupplier(joinInfo, name, rightRowProjectorSupplier);
        } else if (joinInfo.isEquiJoin()) {
            return new JoinByPredicateProcessorSupplier(joinInfo, name, paths, rightRowProjectorSupplier);
        } else {
            return new JoinScanProcessorSupplier(joinInfo, name, rightRowProjectorSupplier);
        }
    }

    private static boolean isEquiJoinByPrimitiveKey(JetJoinInfo joinInfo, List<TableField> fields) {
        if (joinInfo.rightEquiJoinIndices().length != 1) {
            return false;
        }

        MapTableField field = (MapTableField) fields.get(joinInfo.rightEquiJoinIndices()[0]);
        QueryPath path = field.getPath();

        return path.isTop() && path.isKey();
    }
}
