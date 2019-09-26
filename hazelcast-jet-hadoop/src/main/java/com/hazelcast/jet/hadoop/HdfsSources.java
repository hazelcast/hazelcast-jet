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

package com.hazelcast.jet.hadoop;

import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.hadoop.impl.ReadHdfsP.MetaSupplier;
import com.hazelcast.jet.hadoop.impl.ReadNewHdfsP;
import com.hazelcast.jet.hadoop.impl.SerializableConfiguration;
import com.hazelcast.jet.hadoop.impl.SerializableJobConf;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

/**
 * Contains factory methods for Apache Hadoop HDFS sources.
 *
 * @since 3.0
 */
public final class HdfsSources {

    private HdfsSources() {
    }

    /**
     * Returns a source that reads records from Apache Hadoop HDFS using the
     * old MapReduce API {@code org.apache.hadoop.mapred} and emits
     * the results of transforming each record (a key-value pair) with the
     * supplied mapping function.
     * <p>
     * This source splits and balances the input data among Jet {@linkplain
     * Processor processors}, doing its best to achieve
     * data locality. To this end the Jet cluster topology should be aligned
     * with Hadoop's &mdash; on each Hadoop member there should be a Jet
     * member.
     * <p>
     * Default local parallelism for this processor is 2 (or less if less CPUs
     * are available).
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     *
     * @param <K>          key type of the records
     * @param <V>          value type of the records
     * @param <E>          the type of the emitted value
     * @param jobConf      JobConf for reading files with the appropriate input format and path
     * @param projectionFn function to create output objects from key and value.
     *                     If the projection returns a {@code null} for an item, that item
     *                     will be filtered out
     */
    @Nonnull
    public static <K, V, E> BatchSource<E> hdfs(
            @Nonnull JobConf jobConf,
            @Nonnull BiFunctionEx<K, V, E> projectionFn
    ) {
        return Sources.batchFromProcessor("readHdfs", new MetaSupplier<>(SerializableJobConf.asSerializable(jobConf), projectionFn));
    }

    /**
     * Convenience for {@link #hdfs(JobConf, BiFunctionEx)}
     * with {@link java.util.Map.Entry} as its output type.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> hdfs(@Nonnull JobConf jobConf) {
        return hdfs(jobConf, (BiFunctionEx<K, V, Entry<K, V>>) Util::entry);
    }

    /**
     * Returns a source that reads records from Apache Hadoop HDFS using the
     * new MapReduce API {@code org.apache.hadoop.mapreduce} and emits
     * the results of transforming each record (a key-value pair) with the
     * supplied mapping function.
     * <p>
     * This source splits and balances the input data among Jet {@linkplain
     * Processor processors}, doing its best to achieve
     * data locality. To this end the Jet cluster topology should be aligned
     * with Hadoop's &mdash; on each Hadoop member there should be a Jet
     * member.
     * <p>
     * Default local parallelism for this processor is 2 (or less if less CPUs
     * are available).
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     *
     * @param <K>           key type of the records
     * @param <V>           value type of the records
     * @param <E>           the type of the emitted value
     * @param configuration Configuration for reading files with the appropriate input format class and path
     * @param projectionFn  function to create output objects from key and value.
     *                      If the projection returns a {@code null} for an item, that item
     *                      will be filtered out
     */
    @Nonnull
    public static <K, V, E> BatchSource<E> hdfsNewApi(
            @Nonnull Configuration configuration,
            @Nonnull BiFunctionEx<K, V, E> projectionFn
    ) {
        return Sources.batchFromProcessor("readHdfsNew",
                new ReadNewHdfsP.MetaSupplier<>(SerializableConfiguration.asSerializable(configuration), projectionFn));
    }

    /**
     * Convenience for {@link #hdfsNewApi(Configuration, BiFunctionEx)}
     * with {@link java.util.Map.Entry} as its output type.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> hdfsNewApi(@Nonnull Configuration configuration) {
        return hdfsNewApi(configuration, (BiFunctionEx<K, V, Entry<K, V>>) Util::entry);
    }
}
