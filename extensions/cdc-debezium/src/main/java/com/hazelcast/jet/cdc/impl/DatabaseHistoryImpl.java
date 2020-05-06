/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.impl;

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class DatabaseHistoryImpl extends AbstractDatabaseHistory {

    private static final ThreadLocal<Container> CONTEXT = ThreadLocal.withInitial(Container::new);

    private final List<byte[]> history;

    public DatabaseHistoryImpl() {
        this.history = Objects.requireNonNull(container().getHistory());
    }

    public static Container container() {
        return CONTEXT.get();
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        history.add(DocumentWriter.defaultWriter().writeAsBytes(record.document()));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> consumer) {
        try {
            for (byte[] record : history) {
                Document doc = DocumentReader.defaultReader().read(record);
                consumer.accept(new HistoryRecord(doc));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists() {
        return history != null && !history.isEmpty();
    }

    public static class Container {

        @Nullable
        private List<byte[]> history;

        @Nullable
        public List<byte[]> getHistory() {
            return history;
        }

        public void setHistory(@Nullable List<byte[]> history) {
            this.history = history;
        }
    }

}
