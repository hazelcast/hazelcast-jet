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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.sql.impl.schema.ExternalTable;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.explain.QueryExplain;
import com.hazelcast.sql.impl.optimizer.SqlPlan;

import java.util.List;

interface JetPlan extends SqlPlan {

    @Override
    default QueryExplain getExplain() {
        throw new UnsupportedOperationException("TODO");
    }

    SqlResult execute(List<Object> params, long timeout, int pageSize);

    class CreateExternalTablePlan implements JetPlan {

        private final ExternalTable externalTable;
        private final boolean replace;
        private final boolean ifNotExists;
        private final ExecutionPlan executionPlan;
        private final JetSqlServiceImpl sqlService;

        CreateExternalTablePlan(
                ExternalTable externalTable,
                boolean replace,
                boolean ifNotExists,
                ExecutionPlan executionPlan,
                JetSqlServiceImpl sqlService
        ) {
            this.externalTable = externalTable;
            this.replace = replace;
            this.ifNotExists = ifNotExists;
            this.executionPlan = executionPlan;

            this.sqlService = sqlService;
        }

        @Override
        public SqlResult execute(List<Object> params, long timeout, int pageSize) {
            SqlResult result = sqlService.execute(this);
            return executionPlan == null ? result : sqlService.execute(executionPlan);
        }

        ExternalTable externalTable() {
            return externalTable;
        }

        boolean replace() {
            return replace;
        }

        boolean ifNotExists() {
            return ifNotExists;
        }
    }

    class DropExternalTablePlan implements JetPlan {

        private final String name;
        private final boolean ifExists;

        private final JetSqlServiceImpl sqlService;

        DropExternalTablePlan(
                String name,
                boolean ifExists,
                JetSqlServiceImpl sqlService
        ) {
            this.name = name;
            this.ifExists = ifExists;

            this.sqlService = sqlService;
        }

        @Override
        public SqlResult execute(List<Object> params, long timeout, int pageSize) {
            return sqlService.execute(this);
        }

        String name() {
            return name;
        }

        boolean ifExists() {
            return ifExists;
        }
    }

    class CreateJobPlan implements JetPlan {
        private final String name;
        private final JobConfig jobConfig;
        private final boolean ifNotExists;
        private final ExecutionPlan executionPlan;
        private final JetSqlServiceImpl sqlService;

        public CreateJobPlan(String name, JobConfig jobConfig, boolean ifNotExists, ExecutionPlan executionPlan, JetSqlServiceImpl sqlService) {
            this.name = name;
            this.jobConfig = jobConfig;
            this.ifNotExists = ifNotExists;
            this.executionPlan = executionPlan;
            this.sqlService = sqlService;
        }

        @Override
        public SqlResult execute(List<Object> params, long timeout, int pageSize) {
            return sqlService.execute(this);
        }

        public String getName() {
            return name;
        }

        public JobConfig getJobConfig() {
            return jobConfig;
        }

        public boolean isIfNotExists() {
            return ifNotExists;
        }

        public ExecutionPlan getExecutionPlan() {
            return executionPlan;
        }
    }

    class DropJobPlan implements JetPlan {
        private final String name;
        private final boolean ifExists;
        private final JetSqlServiceImpl sqlService;

        public DropJobPlan(String name, boolean ifExists, JetSqlServiceImpl sqlService) {
            this.name = name;
            this.ifExists = ifExists;
            this.sqlService = sqlService;
        }

        @Override
        public SqlResult execute(List<Object> params, long timeout, int pageSize) {
            return sqlService.execute(this);
        }

        public String getName() {
            return name;
        }

        public boolean isIfExists() {
            return ifExists;
        }
    }

    class ExecutionPlan implements JetPlan {

        private final DAG dag;
        private final boolean isStreaming;
        private final boolean isInsert;
        private final QueryId queryId;
        private final SqlRowMetadata rowMetadata;

        private final JetSqlServiceImpl sqlService;

        ExecutionPlan(
                DAG dag,
                boolean isStreaming,
                boolean isInsert,
                QueryId queryId,
                SqlRowMetadata rowMetadata,
                JetSqlServiceImpl sqlService
        ) {
            this.dag = dag;
            this.isStreaming = isStreaming;
            this.isInsert = isInsert;
            this.queryId = queryId;
            this.rowMetadata = rowMetadata;

            this.sqlService = sqlService;
        }

        @Override
        public SqlResult execute(List<Object> params, long timeout, int pageSize) {
            return sqlService.execute(this);
        }

        DAG getDag() {
            return dag;
        }

        boolean isStreaming() {
            return isStreaming;
        }

        boolean isInsert() {
            return isInsert;
        }

        QueryId getQueryId() {
            return queryId;
        }

        SqlRowMetadata getRowMetadata() {
            return rowMetadata;
        }
    }
}
