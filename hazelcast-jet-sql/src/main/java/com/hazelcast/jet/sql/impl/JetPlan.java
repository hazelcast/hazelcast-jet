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

        private final JetSqlBackendImpl jetSqlBackend;

        CreateExternalTablePlan(
                ExternalTable schema,
                boolean replace,
                boolean ifNotExists,
                JetSqlBackendImpl jetSqlBackend
        ) {
            this(schema, replace, ifNotExists, null, jetSqlBackend);
        }

        CreateExternalTablePlan(
                ExternalTable schema,
                boolean replace,
                boolean ifNotExists,
                ExecutionPlan executionPlan,
                JetSqlBackendImpl jetSqlBackend
        ) {
            this.schema = schema;
            this.replace = replace;
            this.ifNotExists = ifNotExists;
            this.executionPlan = executionPlan;

            this.jetSqlBackend = jetSqlBackend;
        }

        @Override
        public SqlResult execute(List<Object> params, long timeout, int pageSize) {
            SqlResult result = jetSqlBackend.execute(this);
            return executionPlan == null ? result : jetSqlBackend.execute(executionPlan);
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

        private final JetSqlBackendImpl jetSqlBackend;

        RemoveExternalTablePlan(
                String name,
                boolean ifExists,
                JetSqlBackendImpl jetSqlBackend
        ) {
            this.name = name;
            this.ifExists = ifExists;

            this.jetSqlBackend = jetSqlBackend;
        }

        @Override
        public SqlResult execute(List<Object> params, long timeout, int pageSize) {
            return jetSqlBackend.execute(this);
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

        private final JetSqlBackendImpl jetSqlBackend;

        ExecutionPlan(
                DAG dag,
                boolean isStreaming,
                boolean isInsert,
                QueryId queryId,
                SqlRowMetadata rowMetadata,
                JetSqlBackendImpl jetSqlBackend
        ) {
            this.dag = dag;
            this.isStreaming = isStreaming;
            this.isInsert = isInsert;
            this.queryId = queryId;
            this.rowMetadata = rowMetadata;

            this.jetSqlBackend = jetSqlBackend;
        }

        @Override
        public SqlResult execute(List<Object> params, long timeout, int pageSize) {
            return jetSqlBackend.execute(this);
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
