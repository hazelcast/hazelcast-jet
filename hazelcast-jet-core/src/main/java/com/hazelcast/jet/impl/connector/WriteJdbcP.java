/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Use {@link SinkProcessors#writeJdbcP}.
 */
public final class WriteJdbcP<T> extends JtaSinkProcessorBase {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, SECONDS.toNanos(1), SECONDS.toNanos(10));
    private static final int BATCH_LIMIT = 50;

    private final CommonDataSource dataSource;
    private final BiConsumerEx<? super PreparedStatement, ? super T> bindFn;
    private final String updateQuery;

    private ILogger logger;
    private XAConnection xaConnection;
    private Connection connection;
    private PreparedStatement statement;
    private List<T> itemList = new ArrayList<>();
    private int idleCount;
    private boolean supportsBatch;
    private int batchCount;

    private WriteJdbcP(
            @Nonnull String updateQuery,
            @Nonnull CommonDataSource dataSource,
            @Nonnull BiConsumerEx<? super PreparedStatement, ? super T> bindFn
    ) {
        super(EXACTLY_ONCE); // TODO [viliam] allow the user to choose
        this.updateQuery = updateQuery;
        this.dataSource = dataSource;
        this.bindFn = bindFn;
    }

    /**
     * Use {@link SinkProcessors#writeJdbcP}.
     */
    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String updateQuery,
            @Nonnull SupplierEx<? extends CommonDataSource> dataSourceSupplier,
            @Nonnull BiConsumerEx<? super PreparedStatement, ? super T> bindFn
    ) {
        checkSerializable(dataSourceSupplier, "newConnectionFn");
        checkSerializable(bindFn, "bindFn");

        return ProcessorMetaSupplier.preferLocalParallelismOne(
                new ProcessorSupplier() {
                    private transient CommonDataSource dataSource;

                    @Override
                    public void init(@Nonnull Context context) {
                        dataSource = dataSourceSupplier.get();
                    }

                    @Nonnull @Override
                    public Collection<? extends Processor> get(int count) {
                        return IntStream.range(0, count)
                                        .mapToObj(i -> new WriteJdbcP<>(updateQuery, dataSource, bindFn))
                                        .collect(Collectors.toList());
                    }
                });
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        connectAndPrepareStatement();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!reconnectIfNecessary()) {
            return;
        }
        // TODO the itemList could be avoided if we could iterate the inbox without removing the items
        itemList.clear();
        inbox.drainTo(itemList);
        try {
            for (T item : itemList) {
                bindFn.accept(statement, (T) item);
                addBatchOrExecute();
            }
            executeBatch();
            if (!snapshotUtility.usesTransactionLifecycle()) {
                connection.commit();
            }
            idleCount = 0;
        } catch (Exception e) {
            if (e instanceof SQLNonTransientException ||
                    e.getCause() instanceof SQLNonTransientException) {
                throw ExceptionUtil.rethrow(e);
            } else {
                logger.warning("Exception during update", e.getCause());
                idleCount++;
            }
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public void close() {
        closeWithLogging(statement);
        closeWithLogging(connection);
    }

    private boolean connectAndPrepareStatement() {
        try {
            if (dataSource instanceof DataSource) {
                if (snapshotUtility.externalGuarantee() == EXACTLY_ONCE) {
                    throw new JetException("When using exactly-once, the dataSource must implement "
                            + XADataSource.class.getName());
                }
                connection = ((DataSource) dataSource).getConnection();
            } else if (dataSource instanceof XADataSource) {
                xaConnection = ((XADataSource) dataSource).getXAConnection();
                connection = xaConnection.getConnection();
                if (snapshotUtility.usesTransactionLifecycle()) {
                    // we never ignore errors in ex-once mode
                    assert idleCount == 0 : "idleCount=" + idleCount;
                    setXaResource(xaConnection.getXAResource());
                }
            } else {
                throw new JetException("The dataSource implements neither " + DataSource.class.getName() + " nor "
                        + XADataSource.class.getName());
            }
            connection.setAutoCommit(false);
            supportsBatch = connection.getMetaData().supportsBatchUpdates();
            statement = connection.prepareStatement(updateQuery);
        } catch (Exception e) {
            logger.warning("Exception during connecting and preparing the statement", e);
            idleCount++;
            return false;
        }
        return true;
    }

    private void addBatchOrExecute() throws SQLException {
        if (!supportsBatch) {
            statement.executeUpdate();
            return;
        }
        statement.addBatch();
        if (++batchCount == BATCH_LIMIT) {
            statement.executeBatch();
            batchCount = 0;
        }
    }

    private void executeBatch() throws SQLException {
        if (supportsBatch && batchCount > 0) {
            statement.executeBatch();
            batchCount = 0;
        }
    }

    private boolean reconnectIfNecessary() {
        if (idleCount == 0) {
            return true;
        }
        IDLER.idle(idleCount);

        close();

        return connectAndPrepareStatement();
    }

    private void closeWithLogging(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            logger.warning("Exception during closing " + closeable, e);
        }
    }
}
