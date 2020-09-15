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
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.SqlResultImpl;

import java.util.Map;

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
        catalog.createMapping(plan.mapping(), plan.replace(), plan.ifNotExists());
        return SqlResultImpl.createUpdateCountResult(-1);
    }

    SqlResult execute(DropExternalMappingPlan plan) {
        catalog.removeMapping(plan.name(), plan.ifExists());
        return SqlResultImpl.createUpdateCountResult(-1);
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
        if (plan.isInsert()) {
            Job job = jetInstance.newJob(plan.getDag());

            if (!plan.isStreaming()) {
                job.join();
            }

            return SqlResultImpl.createUpdateCountResult(-1);
        } else {
            QueryResultProducer queryResultProducer = new JetQueryResultProducer();
            Object oldValue = resultConsumerRegistry.put(plan.getQueryId(), queryResultProducer);
            assert oldValue == null : oldValue;

            jetInstance.newJob(plan.getDag());

            return new JetSqlResultImpl(plan.getQueryId(), queryResultProducer, plan.getRowMetadata());
        }
    }
}
