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
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;
import com.hazelcast.sql.impl.row.Row;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class JetSqlResultImpl extends AbstractSqlResult {
    private final QueryId queryId;
    private final RootResultConsumer rootResultConsumer;
    private final SqlRowMetadata rowMetadata;
    private Iterator<SqlRow> iterator;

    public JetSqlResultImpl(QueryId queryId, RootResultConsumer rootResultConsumer, SqlRowMetadata rowMetadata) {
        this.queryId = queryId;
        this.rootResultConsumer = rootResultConsumer;
        this.rowMetadata = rowMetadata;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public void closeOnError(QueryException exception) {
        rootResultConsumer.onError(exception);
    }

    @Nonnull @Override
    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    @Nonnull @Override
    public Iterator<SqlRow> iterator() {
        if (iterator == null) {
            iterator = new RowToSqlRowIterator(rootResultConsumer.iterator());

            return iterator;
        } else {
            throw new IllegalStateException("Iterator can be requested only once.");
        }
    }

    @Override
    public boolean isUpdateCount() {
        return false;
    }

    @Override
    public long updateCount() {
        throw new IllegalStateException("This result doesn't contain update count");
    }

    private final class RowToSqlRowIterator implements Iterator<SqlRow> {

        private final Iterator<Row> delegate;

        private RowToSqlRowIterator(Iterator<Row> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            try {
                return delegate.hasNext();
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, queryId.getMemberId());
            }
        }

        @Override
        public SqlRow next() {
            try {
                return new SqlRowImpl(getRowMetadata(), delegate.next());
            } catch (NoSuchElementException e) {
                throw e;
            } catch (Exception e) {
                throw QueryUtils.toPublicException(e, queryId.getMemberId());
            }
        }
    }
}
