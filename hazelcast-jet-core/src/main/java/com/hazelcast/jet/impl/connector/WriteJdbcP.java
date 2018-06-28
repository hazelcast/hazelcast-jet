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
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Private API, use {@link SinkProcessors#writeJdbcP}.
 */
public final class WriteJdbcP<T> implements Processor {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MILLISECONDS.toNanos(1), SECONDS.toNanos(1));

    private final DistributedSupplier<Connection> connectionSupplier;
    private final DistributedBiConsumer<PreparedStatement, T> bindFn;
    private final String updateQuery;

    private ILogger logger;
    private Connection connection;
    private PreparedStatement statement;
    private List<T> itemList = new ArrayList<>();
    private int idleCount;

    private WriteJdbcP(
            @Nonnull String updateQuery,
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedBiConsumer<PreparedStatement, T> bindFn
    ) {
        this.updateQuery = updateQuery;
        this.connectionSupplier = connectionSupplier;
        this.bindFn = bindFn;
    }

    /**
     * Private API, use {@link SinkProcessors#writeJdbcP}.
     */
    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String updateQuery,
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedBiConsumer<PreparedStatement, T> bindFn

    ) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(() ->
                new WriteJdbcP<>(updateQuery, connectionSupplier, bindFn));
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        prepareStatement();
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
            for (T item : itemList) {
                bindFn.accept(statement, item);
                statement.addBatch();
            }
            statement.executeBatch();
            itemList.clear();
        } catch (Exception e) {
            if (e instanceof SQLNonTransientException ||
                    e.getCause() instanceof SQLNonTransientException) {
                throw ExceptionUtil.rethrow(e);
            } else {
                logger.warning("Exception during update", e.getCause());
                idleCount++;
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

    private boolean prepareStatement() {
        try {
            connection = connectionSupplier.get();
            statement = connection.prepareStatement(updateQuery);
        } catch (Exception e) {
            logger.warning("Exception during preparing the statement", e);
            idleCount++;
            return false;
        }
        return true;
    }

    private boolean reconnectIfNecessary() {
        if (idleCount == 0) {
            return true;
        }
        IDLER.idle(idleCount);

        closeWithLogging(logger, statement);
        closeWithLogging(logger, connection);

        return prepareStatement();
    }

    private static void closeWithLogging(ILogger logger, AutoCloseable closeable) {
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
}
