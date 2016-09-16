/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sink;

import com.hazelcast.core.IList;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.impl.dag.sink.ListPartitionWriter;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.runtime.DataWriter;

/**
 * A sink which uses a Hazelcast {@code IList} as output.
 */
public class ListSink implements Sink {
    private final String name;

    /**
     * Constructs a sink with the given list name.
     *
     * @param name of the list to use as the output
     */
    public ListSink(String name) {
        this.name = name;
    }

    /**
     * Constructs a sink with the given list instance.
     *
     * @param list the list instance to be used as the output
     */
    public ListSink(IList list) {
        this(list.getName());
    }

    @Override
    public DataWriter[] getWriters(JobContext jobContext) {
        return new DataWriter[]{ new ListPartitionWriter(jobContext, name)};
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String toString() {
        return "ListSink{"
                + "name='" + name + '\''
                + '}';
    }
}
