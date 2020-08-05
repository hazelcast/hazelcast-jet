package com.hazelcast.jet.sql.impl;

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

        private final ExternalTable schema;
        private final boolean replace;
        private final boolean ifNotExists;
        private final ExecutionPlan executionPlan;

        private final JetSqlServiceImpl sqlService;

        CreateExternalTablePlan(
                ExternalTable schema,
                boolean replace,
                boolean ifNotExists,
                JetSqlServiceImpl sqlService
        ) {
            this(schema, replace, ifNotExists, null, sqlService);
        }

        CreateExternalTablePlan(
                ExternalTable schema,
                boolean replace,
                boolean ifNotExists,
                ExecutionPlan executionPlan,
                JetSqlServiceImpl sqlService
        ) {
            this.schema = schema;
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

        ExternalTable schema() {
            return schema;
        }

        boolean replace() {
            return replace;
        }

        boolean ifNotExists() {
            return ifNotExists;
        }
    }

    class RemoveExternalTablePlan implements JetPlan {

        private final String name;
        private final boolean ifExists;

        private final JetSqlServiceImpl sqlService;

        RemoveExternalTablePlan(
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
