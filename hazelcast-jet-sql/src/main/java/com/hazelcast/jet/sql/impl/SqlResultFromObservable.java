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

import com.hazelcast.jet.Observable;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.HeapRow;

import javax.annotation.Nonnull;
import java.util.Iterator;

public class SqlResultFromObservable implements SqlResult {

    private final SqlRowMetadata rowMetadata;
    private final Observable<Object[]> observable;

    private Iterator<SqlRow> iterator;

    public SqlResultFromObservable(SqlRowMetadata rowMetadata, Observable<Object[]> observable) {
        this.rowMetadata = rowMetadata;
        this.observable = observable;

        Iterator<Object[]> observableIt = observable.iterator();
        iterator = new Iterator<SqlRow>() {
            @Override
            public boolean hasNext() {
                return observableIt.hasNext();
            }

            @Override
            public SqlRow next() {
                return new SqlRowImpl(new HeapRow(observableIt.next()));
            }
        };
    }

    @Override
    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    @Nonnull @Override
    public Iterator<SqlRow> iterator() {
        if (iterator == null) {
            throw QueryException.error("Iterator can be requested only once.");
        }
        try {
            return iterator;
        } finally {
            iterator = null;
        }
    }

    @Override
    public void close() {
        observable.destroy();
    }
}
