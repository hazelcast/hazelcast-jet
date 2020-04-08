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

package com.hazelcast.jet.cdc;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.util.Preconditions;
import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Database history implementation backed by Hazelcast IList {@link IList}.
 *
 * @since 4.1
 */
public class HazelcastListDatabaseHistory extends AbstractDatabaseHistory {

    /**
     * Hazelcast IList {@link IList} name property.
     */
    public static final String LIST_NAME_PROPERTY = "database.history.hazelcast.list.name";

    private String listName;
    private HazelcastInstance instance;
    private IList<byte[]> list;

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator,
                          DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        String instanceName = config.getString("database.history.hazelcast.instance.name");
        this.instance = HazelcastInstanceFactory.getHazelcastInstance(instanceName);
        listName = config.getString(LIST_NAME_PROPERTY);
        Preconditions.checkHasText(listName, LIST_NAME_PROPERTY + " property cannot have null value");
    }

    @Override
    public void start() {
        super.start();
        list = instance.getList(listName);
    }


    @Override
    protected void storeRecord(HistoryRecord historyRecord) throws DatabaseHistoryException {
        list.add(writer.writeAsBytes(historyRecord.document()));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> consumer) {
        try {
            for (byte[] r : list) {
                Document doc = reader.read(r);
                consumer.accept(new HistoryRecord(doc));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public boolean exists() {
        return !instance.getList(listName).isEmpty();
    }

    @Override
    public void initializeStorage() {
        instance.getList(listName).clear();
    }
}
