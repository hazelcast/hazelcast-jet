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

import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Private API, use {@link SinkProcessors#writeJdbcP}.
 */
public final class WriteJdbcP<T> implements Processor {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MILLISECONDS.toNanos(1), SECONDS.toNanos(10));

    private final DistributedSupplier<Connection> connectionSupplier;
    private final DistributedFunction<Connection, PreparedStatement> statementFn;
    private final DistributedBiConsumer<PreparedStatement, T> updateFn;
    private final DistributedBiConsumer<Connection, PreparedStatement> flushFn;

    private ILogger logger;
    private Connection connection;
    private PreparedStatement statement;
    private List<T> itemList = new ArrayList<>();
    private int idleCount;

    public WriteJdbcP(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, PreparedStatement> statementFn,
            @Nonnull DistributedBiConsumer<PreparedStatement, T> updateFn,
            @Nonnull DistributedBiConsumer<Connection, PreparedStatement> flushFn
    ) {
        this.connectionSupplier = connectionSupplier;
        this.statementFn = statementFn;
        this.updateFn = updateFn;
        this.flushFn = flushFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        connection = connectionSupplier.get();
        statement = statementFn.apply(connection);
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!reconnectIfNecessary()) {
            return;
        }
        if (itemList.isEmpty()) {
            inbox.drainTo(itemList);
        }
        try {
            itemList.forEach(item -> updateFn.accept(statement, item));
            flushFn.accept(connection, statement);
            itemList.clear();
        } catch (Exception e) {
            if (e.getCause() instanceof SQLNonTransientException) {
                logger.warning("Exception during update", e.getCause());
                idleCount++;
            } else {
                throw ExceptionUtil.rethrow(e);
            }
        }
        idleCount = 0;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public void close() throws Exception {
        Exception stmtException = close(statement);
        Exception connectionException = close(connection);
        if (stmtException != null) {
            throw stmtException;
        }
        if (connectionException != null) {
            throw connectionException;
        }
    }

    private boolean reconnectIfNecessary() {
        if (idleCount == 0) {
            return true;
        }
        IDLER.idle(idleCount);

        closeSilently(statement);
        closeSilently(connection);

        try {
            connection = connectionSupplier.get();
            statement = statementFn.apply(connection);
        } catch (Exception e) {
            logger.warning("Exception during reconnection", e);
            idleCount++;
            return false;
        }
        return true;
    }

    private void closeSilently(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            logger.warning("Exception during closing " + closeable, e);
        }
    }

    private static Exception close(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            return e;
        }
        return null;
    }

    /**
     * Private API, use {@link SinkProcessors#writeJdbcP}.
     */
    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, PreparedStatement> statementFn,
            @Nonnull DistributedBiConsumer<PreparedStatement, T> updateFn,
            @Nonnull DistributedBiConsumer<Connection, PreparedStatement> flushFn

    ) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(() ->
                new WriteJdbcP<>(connectionSupplier, statementFn, updateFn, flushFn));
    }

    /**
     * Private API, use {@link
     * SinkProcessors#writeJdbcP(String, String, DistributedBiConsumer)}.
     */
    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String connectionUrl,
            @Nonnull String updateQuery,
            @Nonnull DistributedBiConsumer<PreparedStatement, T> bindFn
    ) {
        DistributedSupplier<Connection> connectionSupplier = () -> uncheckCall(() -> {
            Connection connection = DriverManager.getConnection(connectionUrl);
            connection.setAutoCommit(false);
            return connection;
        });
        DistributedFunction<Connection, PreparedStatement> statementFn =
                connection -> uncheckCall(() -> connection.prepareStatement(updateQuery));
        return ProcessorMetaSupplier.preferLocalParallelismOne(() -> new WriteJdbcP<>(connectionSupplier, statementFn,
                (stmt, item) -> {
                    bindFn.accept(stmt, (T) item);
                    uncheckRun(stmt::addBatch);
                },
                (connection, stmt) -> uncheckRun(() -> {
                    stmt.executeBatch();
                    connection.commit();
                })
        ));
    }
}
