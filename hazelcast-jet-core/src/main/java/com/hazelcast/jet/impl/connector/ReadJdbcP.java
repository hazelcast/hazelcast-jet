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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * Private API, use {@link SourceProcessors#readJdbcP}.
 */
public final class ReadJdbcP<T> extends AbstractProcessor {

    private final DistributedSupplier<Connection> connectionSupplier;
    private final DistributedFunction<Connection, Statement> statementFn;
    private final DistributedBiFunction<Integer, Integer, String> sqlFn;
    private final DistributedFunction<ResultSet, T> mapOutputFn;

    private Connection connection;
    private Statement statement;
    private String sql;
    private Traverser traverser;

    private ReadJdbcP(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, Statement> statementFn,
            @Nonnull DistributedBiFunction<Integer, Integer, String> sqlFn,
            @Nonnull DistributedFunction<ResultSet, T> mapOutputFn
    ) {
        this.connectionSupplier = connectionSupplier;
        this.statementFn = statementFn;
        this.sqlFn = sqlFn;
        this.mapOutputFn = mapOutputFn;
        setCooperative(false);
    }

    /**
     * Private API, use {@link SourceProcessors#readJdbcP}.
     */
    public static <T> ProcessorSupplier supplier(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, Statement> statementFn,
            @Nonnull DistributedBiFunction<Integer, Integer, String> sqlFn,
            @Nonnull DistributedFunction<ResultSet, T> mapOutputFn
    ) {
        return ProcessorSupplier.of(() -> new ReadJdbcP<>(connectionSupplier, statementFn, sqlFn, mapOutputFn));
    }

    public static <T> ProcessorSupplier supplier(
            @Nonnull String connectionURL, @Nonnull String query,
            @Nonnull DistributedFunction<ResultSet, T> mapOutputFn
    ) {
        return ProcessorSupplier.of(() -> new ReadJdbcP<>(
                () -> uncheckCall(() -> DriverManager.getConnection(connectionURL)),
                connection -> uncheckCall(connection::createStatement),
                (parallelism, index) -> query, mapOutputFn));
    }

    @Override
    protected void init(@Nonnull Context context) {
        connection = connectionSupplier.get();
        statement = statementFn.apply(connection);
        sql = sqlFn.apply(context.totalParallelism(), context.globalProcessorIndex());
    }

    @Override
    public boolean complete() {
        if (traverser == null) {
            ResultSet resultSet = uncheckCall(() -> statement.executeQuery(sql));
            traverser = ((Traverser<ResultSet>) () -> uncheckCall(() -> resultSet.next() ? resultSet : null))
                    .map(mapOutputFn)
                    .onFirstNull(() -> uncheckRun(resultSet::close));
        }
        return emitFromTraverser(traverser);
    }

    @Override
    public void close(@Nullable Throwable error) throws SQLException {
        SQLException statementException = null;
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            statementException = e;
        }
        if (connection != null) {
            connection.close();
        }
        if (statementException != null) {
            throw statementException;
        }
    }
}
