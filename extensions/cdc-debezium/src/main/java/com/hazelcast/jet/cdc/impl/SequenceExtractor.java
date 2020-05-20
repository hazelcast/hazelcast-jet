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

package com.hazelcast.jet.cdc.impl;

import java.util.Map;

/**
 * Utility that ingests Debezium event headers and computes a sequence
 * number we can use to ensure the ordering of {@code ChangeRecord} items.
 * <p>
 * The <i>sequence</i> part is exactly what the name implies: a numeric
 * sequence which we base our ordering on. Implementations needs to ensure
 * that {@code ChangeRecord}s produced by a source contain a monotonic
 * increasing sequence number, as long as the sequnce number partition
 * doesn't change.
 * <p>
 * The <i>partition</i> part is a kind of context for the numeric
 * sequence, the "source" of it if you will. It is necessary for avoiding
 * the comparison of numeric sequences which come from different sources.
 * For example if the numeric sequence is in fact based on transaction
 * IDs, then it makes sense to compare them only if they are produced by
 * the same database instance. Or if the numeric ID is an offset in a
 * write-ahead log, then it makes sense to compare them only if they are
 * offsets from the same log file. Implementations need to make sure that
 * the partition is the same if and only if the source of the numeric
 * sequence is the same.
 */
public interface SequenceExtractor {

    long partition(Map<String, ?> debeziumPartition, Map<String, ?> debeziumOffset);

    long sequence(Map<String, ?> debeziumOffset);

}
