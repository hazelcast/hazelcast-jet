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

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.sql.impl.JetPlan.CreateExternalTablePlan;
import com.hazelcast.jet.sql.impl.JetPlan.ExecutionPlan;
import com.hazelcast.jet.sql.impl.JetPlan.RemoveExternalTablePlan;
import com.hazelcast.jet.sql.impl.schema.ExternalCatalog;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.JetSqlService;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.schema.TableResolver;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

@SuppressWarnings("unused") // used through reflection
public class JetSqlServiceImpl implements JetSqlService, ManagedService {

    private JetInstance jetInstance;
    private Map<QueryId, RootResultConsumer> resultConsumerRegistry;

    private ExternalCatalog catalog;
    private JetSqlBackendImpl sqlBackend;

    @SuppressWarnings("unused") // used through reflection
    public void initJetInstance(@Nonnull JetInstance jetInstance) {
        this.jetInstance = Objects.requireNonNull(jetInstance);
        NodeEngine nodeEngine = ((HazelcastInstanceImpl) jetInstance.getHazelcastInstance()).node.nodeEngine;

        JetService jetService = nodeEngine.getService(JetService.SERVICE_NAME);
        this.resultConsumerRegistry = jetService.getResultConsumerRegistry();

        this.catalog = new ExternalCatalog(nodeEngine);

        this.sqlBackend = new JetSqlBackendImpl(nodeEngine, this, catalog);
    }

    @Override
    public List<TableResolver> tableResolvers() {
        return Collections.singletonList(catalog);
    }

    @Override
    public Object sqlBackend() {
        return sqlBackend;
    }

    @Override
    public SqlResult execute(SqlPlan plan, List<Object> params, long timeout, int pageSize) {
        if (params != null && !params.isEmpty()) {
            throw new JetException("Query parameters not yet supported");
        }
        if (timeout > 0) {
            throw new JetException("Query timeout not supported");
        }

        return ((JetPlan) plan).execute(params, timeout, pageSize);
    }

    SqlResult execute(ExecutionPlan plan) {
        RootResultConsumer consumer;
        if (plan.isInsert()) {
            consumer = null;
        } else {
            consumer = new BlockingRootResultConsumer();
            consumer.setup(() -> { });
            Object oldValue = resultConsumerRegistry.put(plan.getQueryId(), consumer);
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
            return new JetSqlResultImpl(plan.getQueryId(), consumer, plan.getRowMetadata());
        }
    }

    SqlResult execute(CreateExternalTablePlan plan) {
        catalog.createTable(plan.schema(), plan.replace(), plan.ifNotExists());
        return SqlResultImpl.createUpdateCountResult(-1);
    }

    SqlResult execute(RemoveExternalTablePlan plan) {
        catalog.removeTable(plan.name(), plan.ifExists());
        return SqlResultImpl.createUpdateCountResult(-1);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }
}
