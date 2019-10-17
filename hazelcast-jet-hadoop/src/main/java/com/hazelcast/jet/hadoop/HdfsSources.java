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
import com.hazelcast.jet.hadoop.impl.SerializableConfiguration;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.hadoop.HdfsProcessors.readHdfsP;

/**
 * Contains factory methods for Apache Hadoop HDFS sources.
 *
 * @since 3.0
 */
public final class HdfsSources {

    /**
     * With the new HDFS API, some of the {@link RecordReader}s return the same
     * key/value objects for each record, for example {@link LineRecordReader}.
     * The source makes a copy of each object ot emit them to downstream. But
     * for the readers which creates a new object for each record, the source
     * can be configured to not copy the objects but to reuse them.
     * <p>
     * Also if you are using a mapper function which creates a new object for
     * each record then it makes sense to set this property to {@code true} to
     * avoid unnecessary copying.
     * <p>
     * The source copies the objects by serializing and de-serializing it. The
     * objects should be either {@link Writable} or serializable in a way that
     * Jet instance can serialize/de-serialize.
     * <p>
     * Here is how you can configure the source. Default value is {@code false}.
     * <pre>{@code
     * Configuration conf = new Configuration();
     * conf.set(HdfsSources.REUSE_OBJECT, "true");
     *
     * BatchSource<Entry<K, V>> source = HdfsSources.hdfs(conf);
     * }</pre>
     */
    public static final String REUSE_OBJECT = "REUSE_OBJECTS_FOR_JET_HDFS_SOURCE";

    private HdfsSources() {
    }

    /**
     * Returns a source that reads records from Apache Hadoop HDFS and emits
     * the results of transforming each record (a key-value pair) with the
     * supplied projection function.
     * <p>
     * This source splits and balances the input data among Jet {@linkplain
     * Processor processors}, doing its best to achieve data locality. To this
     * end the Jet cluster topology should be aligned with Hadoop's &mdash; on
     * each Hadoop member there should be a Jet member.
     * <p>
     * The processor will use either the new or the old MapReduce API based on
     * the key which stores the {@code InputFormat} configuration. If it's
     * stored under {@value MRJobConfig#INPUT_FORMAT_CLASS_ATTR}, the new API
     * will be used. Otherwise, the old API will be used. If you get the
     * configuration from {@link Job#getConfiguration()}, the new API will be
     * used. Please see {@link #REUSE_OBJECT} if you are using the new API.
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
     * @param configuration JobConf for reading files with the appropriate
     *                      input format and path
     * @param projectionFn  function to create output objects from key and value.
     *                      If the projection returns a {@code null} for an item, that item
     *                      will be filtered out
     */
    @Nonnull
    public static <K, V, E> BatchSource<E> hdfs(
            @Nonnull Configuration configuration,
            @Nonnull BiFunctionEx<K, V, E> projectionFn
    ) {
        return Sources.batchFromProcessor("readHdfs",
                readHdfsP(SerializableConfiguration.asSerializable(configuration), projectionFn));
    }

    /**
     * Convenience for {@link #hdfs(Configuration, BiFunctionEx)}
     * with {@link java.util.Map.Entry} as its output type.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> hdfs(@Nonnull Configuration jobConf) {
        return hdfs(jobConf, (BiFunctionEx<K, V, Entry<K, V>>) Util::entry);
    }
}
