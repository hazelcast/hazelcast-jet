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

import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.sql.impl.plan.cache.PlanObjectId;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import javax.annotation.Nonnull;
import java.util.List;

public abstract class JetTable extends Table {

    private final SqlConnector sqlConnector;

    protected JetTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull TableStatistics statistics
    ) {
        super(schemaName, name, fields, statistics);
        this.sqlConnector = sqlConnector;
    }

    @Override
    public final PlanObjectId getObjectId() {
        return null;
    }

    public final boolean isStream() {
        return sqlConnector.isStream();
    }

    @Nonnull
    public SqlConnector getSqlConnector() {
        return sqlConnector;
    }
}
