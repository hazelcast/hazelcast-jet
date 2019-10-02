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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

/**
 * Factories of Apache Hadoop HDFS sinks.
 *
 * @since 3.0
 */
public final class HdfsSinks {

    private HdfsSinks() {
    }

    /**
     * Returns a sink that writes to Apache Hadoop HDFS. It transforms each
     * received item to a key-value pair using the two supplied mapping
     * functions. The type of the key and the value must conform to the
     * expectations of the output format specified in the {@code
     * configuration}.
     * <p>
     * The sink creates a number of files in the output path, identified by the
     * cluster member UUID and the {@link Processor} index. Unlike MapReduce,
     * the data in the files is not sorted by key.
     * <p>
     * The supplied {@code Configuration} must specify an {@code OutputFormat}
     * class with a path.
     * <p>
     * The processor will use either the new or the old MapReduce API based on
     * the key which stores the {@code OutputFormat} configuration. If it's
     * stored under {@value MRJobConfig#OUTPUT_FORMAT_CLASS_ATTR}}, the new API
     * will be used. Otherwise, the old API will be used. If you get the
     * configuration from {@link Job#getConfiguration()}, the new API will be
     * used.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee. TODO [viliam] wrong, don't we overwrite files?
     * <p>
     * Default local parallelism for this processor is 2 (or less if less CPUs
     * are available).
     *
     * @param configuration {@code Configuration} used for output format configuration
     * @param extractKeyF   mapper to map a key to another key
     * @param extractValueF mapper to map a value to another value
     * @param <E>           stream item type
     * @param <K>           type of key to write to HDFS
     * @param <V>           type of value to write to HDFS
     */
    @Nonnull
    public static <E, K, V> Sink<E> hdfs(
            @Nonnull Configuration configuration,
            @Nonnull FunctionEx<? super E, K> extractKeyF,
            @Nonnull FunctionEx<? super E, V> extractValueF
    ) {
        return Sinks.fromProcessor("writeHdfs", HdfsProcessors.writeHdfsP(configuration, extractKeyF, extractValueF));
    }

    /**
     * Convenience for {@link #hdfs(Configuration, FunctionEx,
     * FunctionEx)} which expects {@code Map.Entry<K, V>} as
     * input and extracts its key and value parts to be written to HDFS.
     */
    @Nonnull
    public static <K, V> Sink<Entry<K, V>> hdfs(@Nonnull Configuration configuration) {
        return hdfs(configuration, Entry::getKey, Entry::getValue);
    }
}
