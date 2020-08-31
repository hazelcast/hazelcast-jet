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

import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.jet.sql.impl.connector.map.JetMapMetadataResolverImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.JetSqlCoreBackend;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.JetMapMetadataResolver;

import java.util.List;
import java.util.Properties;

import static java.util.Collections.emptyList;

public class JetSqlCoreBackendImpl implements JetSqlCoreBackend, ManagedService {

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @SuppressWarnings("unused") // used through reflection
    public void init() {
    }

    @Override
    public List<TableResolver> tableResolvers() {
        return emptyList();
    }

    @Override
    public JetMapMetadataResolver mapMetadataResolver() {
        return JetMapMetadataResolverImpl.INSTANCE;
    }

    @Override
    public Object sqlBackend() {
        return null;
    }

    @Override
    public SqlResult execute(SqlPlan plan, List<Object> params, long timeout, int pageSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }
}
