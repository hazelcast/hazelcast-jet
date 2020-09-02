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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.sql.impl.JetPlan.CreateExternalMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropExternalMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.ExecutionPlan;
import com.hazelcast.jet.sql.impl.JetPlan.ShowExternalMappingsPlan;
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.HeapRow;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

class JetPlanExecutor {

    private final MappingCatalog catalog;
    private final JetInstance jetInstance;
    private final Map<QueryId, QueryResultProducer> resultConsumerRegistry;

    JetPlanExecutor(
            MappingCatalog catalog,
            JetInstance jetInstance,
            Map<QueryId, QueryResultProducer> resultConsumerRegistry
    ) {
        this.catalog = catalog;
        this.jetInstance = jetInstance;
        this.resultConsumerRegistry = resultConsumerRegistry;
    }

    SqlResult execute(CreateExternalMappingPlan plan) {
        catalog.createMapping(plan.externalTable(), plan.replace(), plan.ifNotExists());
        return SqlResultImpl.createUpdateCountResult(-1);
    }

    SqlResult execute(DropExternalMappingPlan plan) {
        catalog.removeMapping(plan.name(), plan.ifExists());
        return SqlResultImpl.createUpdateCountResult(-1);
    }

    SqlResult execute(@SuppressWarnings("unused") ShowExternalMappingsPlan plan) {
        SqlRowMetadata metadata = new SqlRowMetadata(asList(
                new SqlColumnMetadata("name", SqlColumnType.VARCHAR),
                new SqlColumnMetadata("ddl", SqlColumnType.VARCHAR)));
        List<SqlRow> rows = catalog.getMappings()
            .map(table -> new SqlRowImpl(metadata, new HeapRow(new Object[]{table.name(), table.ddl()})))
            .collect(toList());

        return new JetStaticSqlResultImpl(
                QueryId.create(jetInstance.getHazelcastInstance().getLocalEndpoint().getUuid()),
                rows,
                metadata
        );
    }

    SqlResult execute(CreateJobPlan plan) {
        if (plan.isIfNotExists()) {
            jetInstance.newJobIfAbsent(plan.getExecutionPlan().getDag(), plan.getJobConfig());
        } else {
            jetInstance.newJob(plan.getExecutionPlan().getDag(), plan.getJobConfig());
        }
        return SqlResultImpl.createUpdateCountResult(-1);
    }

    SqlResult execute(DropJobPlan plan) {
        Job job = jetInstance.getJob(plan.getName());
        if (job == null || job.getStatus().isTerminal()) {
            if (plan.isIfExists()) {
                return SqlResultImpl.createUpdateCountResult(-1);
            }
            throw QueryException.error("Job doesn't exist or already terminated: " + plan.getName());
        }
        job.cancel();
        return SqlResultImpl.createUpdateCountResult(-1);
    }

    SqlResult execute(ExecutionPlan plan) {
        QueryResultProducer queryResultProducer;
        if (plan.isInsert()) {
            queryResultProducer = null;
        } else {
            queryResultProducer = new JetQueryResultProducer();
            Object oldValue = resultConsumerRegistry.put(plan.getQueryId(), queryResultProducer);
            assert oldValue == null : oldValue;
        }

        // submit the job
        Job job = jetInstance.newJob(plan.getDag());

        if (plan.isInsert()) {
            if (plan.isStreaming()) {
                return SqlResultImpl.createUpdateCountResult(-1);
            } else {
                job.join();
                // TODO return real updated row count
                return SqlResultImpl.createUpdateCountResult(-1);
            }
        } else {
            return new JetDynamicSqlResultImpl(plan.getQueryId(), queryResultProducer, plan.getRowMetadata());
        }
    }
}
