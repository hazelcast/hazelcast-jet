/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.hazelcast.jet.core.processor.SinkProcessors.writeBufferedP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * Private API, use {@link SinkProcessors#writeJdbcP}.
 */
public final class WriteJdbcP {

    private WriteJdbcP() {
    }

    /**
     * Private API, use {@link SinkProcessors#writeJdbcP}.
     */
    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, Statement> statementFn,
            @Nonnull DistributedBiConsumer<Statement, T> updateFn,
            @Nonnull DistributedBiConsumer<Connection, Statement> flushFn

    ) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(writeBufferedP(
                context -> new JdbcContext(connectionSupplier, statementFn),
                (jdbcContext, o) -> updateFn.accept(jdbcContext.statement, (T) o),
                jdbcContext -> flushFn.accept(jdbcContext.connection, jdbcContext.statement),
                jdbcContext -> uncheckRun(jdbcContext::close)
        ));
    }

    private static final class JdbcContext {

        private final Connection connection;
        private final Statement statement;

        JdbcContext(DistributedSupplier<Connection> connectionSupplier,
                    DistributedFunction<Connection, Statement> statementFn) {
            this.connection = connectionSupplier.get();
            this.statement = statementFn.apply(connection);
        }

        private void close() throws SQLException {
            SQLException statementException = null;
            try {
                statement.close();
            } catch (SQLException e) {
                statementException = e;
            }
            connection.close();
            if (statementException != null) {
                throw statementException;
            }
        }
    }
}
