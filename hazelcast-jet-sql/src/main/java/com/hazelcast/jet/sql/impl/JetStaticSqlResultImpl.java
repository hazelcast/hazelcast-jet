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

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.AbstractSqlResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.ResultIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JetStaticSqlResultImpl extends AbstractSqlResult {

    private final QueryId queryId;
    private final List<SqlRow> rows;
    private final SqlRowMetadata rowMetadata;

    private ResultIterator<SqlRow> iterator;

    public JetStaticSqlResultImpl(QueryId queryId, List<SqlRow> rows, SqlRowMetadata rowMetadata) {
        this.queryId = queryId;
        this.rows = rows;
        this.rowMetadata = rowMetadata;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    @Nonnull
    @Override
    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    @Nonnull
    @Override
    public ResultIterator<SqlRow> iterator() {
        if (iterator == null) {
            iterator = new ResultIteratorFromIterator<>(rows.iterator());

            return iterator;
        } else {
            throw new IllegalStateException("Iterator can be requested only once.");
        }
    }

    @Override
    public long updateCount() {
        throw new IllegalStateException("This result doesn't contain update count");
    }

    @Override
    public void close(@Nullable QueryException exception) {
    }

    private static class ResultIteratorFromIterator<T> implements ResultIterator<T> {
        private final Iterator<T> delegate;

        ResultIteratorFromIterator(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
            return hasNext() ? HasNextResult.YES : HasNextResult.DONE;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public T next() {
            return delegate.next();
        }
    }
}
