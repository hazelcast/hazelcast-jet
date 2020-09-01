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

package com.hazelcast.jet.impl;

import com.hazelcast.sql.impl.JetSqlCoreBackend;

import javax.annotation.Nullable;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

final class JetSqlCoreBackendFactory {

    private static final String SQL_CORE_BACKEND_CLASS_NAME = "com.hazelcast.jet.sql.impl.JetSqlCoreBackendImpl";

    private static final Class<?> SQL_CORE_BACKEND_CLASS;

    static {
        Class<?> sqlCoreBackendClass;
        try {
            sqlCoreBackendClass = Class.forName(SQL_CORE_BACKEND_CLASS_NAME);
        } catch (ClassNotFoundException e) {
            sqlCoreBackendClass = null;
        }
        SQL_CORE_BACKEND_CLASS = sqlCoreBackendClass;
    }

    private JetSqlCoreBackendFactory() {
    }

    @Nullable
    static JetSqlCoreBackend createJetSqlCoreBackend() {
        if (SQL_CORE_BACKEND_CLASS == null) {
            return null;
        }

        try {
            return (JetSqlCoreBackend) SQL_CORE_BACKEND_CLASS.newInstance();
        } catch (ReflectiveOperationException e) {
            throw sneakyThrow(e);
        }
    }
}
