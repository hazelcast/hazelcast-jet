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

import com.hazelcast.jet.sql.JetSqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.plan.cache.PlanObjectId;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;

class FileTable extends JetTable {

    private final String directory;
    private final String glob;
    private final boolean sharedFileSystem;

    private final TargetDescriptor targetDescriptor;

    FileTable(
            JetSqlConnector sqlConnector,
            String schemaName,
            String name,
            String directory,
            String glob,
            boolean sharedFileSystem,
            List<TableField> fields,
            TargetDescriptor targetDescriptor
    ) {
        super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(0));

        this.directory = directory;
        this.glob = glob;
        this.sharedFileSystem = sharedFileSystem;

        this.targetDescriptor = targetDescriptor;
    }

    String getDirectory() {
        return directory;
    }

    String getGlob() {
        return glob;
    }

    boolean isSharedFileSystem() {
        return sharedFileSystem;
    }

    TargetDescriptor getTargetDescriptor() {
        return targetDescriptor;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{directory=" + directory + '}';
    }

    @Override
    public PlanObjectId getObjectId() {
        throw new UnsupportedOperationException();
    }
}
