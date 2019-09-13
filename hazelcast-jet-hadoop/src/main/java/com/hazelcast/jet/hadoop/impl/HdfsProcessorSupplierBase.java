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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.jet.core.ProcessorSupplier;

import com.hazelcast.jet.function.BiFunctionEx;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * Base class contains shared logic between HDFS processor suppliers which are using
 * old and new MapReduce API.
 */
abstract class HdfsProcessorSupplierBase<K, V, R> implements ProcessorSupplier {

    SerializableJobConf jobConf;
    BiFunctionEx<K, V, R> mapper;
    List<IndexedInputSplit> assignedSplits;


    HdfsProcessorSupplierBase(SerializableJobConf jobConf, List<IndexedInputSplit> assignedSplits,
                              BiFunctionEx<K, V, R> mapper) {
        this.jobConf = jobConf;
        this.assignedSplits = assignedSplits;
        this.mapper = mapper;
    }

    Map<Integer, List<IndexedInputSplit>> getProcessorToSplits(int count) {
        Map<Integer, List<IndexedInputSplit>> processorToSplits =
                range(0, assignedSplits.size()).mapToObj(i -> new SimpleImmutableEntry<>(i, assignedSplits.get(i)))
                                               .collect(groupingBy(e -> e.getKey() % count,
                                                       mapping(Entry::getValue, toList())));
        range(0, count)
                .forEach(processor -> processorToSplits.computeIfAbsent(processor, x -> emptyList()));
        return processorToSplits;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        jobConf.write(out);
        out.writeObject(assignedSplits);
        out.writeObject(mapper);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        jobConf = new SerializableJobConf();
        jobConf.readFields(in);
        assignedSplits = (List<IndexedInputSplit>) in.readObject();
        mapper = (BiFunctionEx<K, V, R>) in.readObject();
    }


}
