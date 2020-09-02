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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;

class FileTable extends JetTable {

    private final TargetDescriptor targetDescriptor;

    FileTable(
            SqlConnector sqlConnector,
            String schemaName,
            String name,
            List<TableField> fields,
            TargetDescriptor targetDescriptor
    ) {
        super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(0));

        this.targetDescriptor = targetDescriptor;
    }

    ProcessorMetaSupplier readProcessor(Expression<Boolean> predicate, List<Expression<?>> projection) {
        return targetDescriptor.readProcessor(getFields(), predicate, projection);
    }

    ProcessorSupplier projectionProcessor() {
        return targetDescriptor.projectorProcessor(getFields());
    }

    ProcessorMetaSupplier writeProcessor() {
        return targetDescriptor.writeProcessor(getFields());
    }
}
