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

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.hadoop.impl.ReadHdfsNewApiP;
import com.hazelcast.jet.hadoop.impl.ReadHdfsOldApiP;
import com.hazelcast.jet.hadoop.impl.SerializableConfiguration;
import com.hazelcast.jet.hadoop.impl.WriteHdfsNewApiP;
import com.hazelcast.jet.hadoop.impl.WriteHdfsOldApiP;
import org.apache.hadoop.conf.Configuration;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;

import javax.annotation.Nonnull;

/**
 * Static utility class with factories of Apache Hadoop HDFS source and sink
 * processors.
 *
 * @since 3.0
 */
public final class HdfsProcessors {

    private HdfsProcessors() {
    }

    /**
     * Returns a supplier of processors for
     * {@link HdfsSources#hdfs(Configuration, BiFunctionEx)}.
     */
    @Nonnull
    public static <K, V, R> ProcessorMetaSupplier readHdfsP(
            @Nonnull Configuration configuration, @Nonnull BiFunctionEx<K, V, R> projectionFn
    ) {
        configuration = SerializableConfiguration.asSerializable(configuration);
        if (configuration.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR) != null) {
            return new ReadHdfsNewApiP.MetaSupplier<>(configuration, projectionFn);
        } else {
            return new ReadHdfsOldApiP.MetaSupplier<>((JobConf) configuration, projectionFn);
        }
    }

    /**
     * Returns a supplier of processors for
     * {@link HdfsSinks#hdfs(Configuration, FunctionEx, FunctionEx)}.
     */
    @Nonnull
    public static <E, K, V> ProcessorMetaSupplier writeHdfsP(
            @Nonnull Configuration configuration,
            @Nonnull FunctionEx<? super E, K> extractKeyFn,
            @Nonnull FunctionEx<? super E, V> extractValueFn
    ) {
        configuration = SerializableConfiguration.asSerializable(configuration);
        if (configuration.get(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR) != null) {
            return new WriteHdfsNewApiP.MetaSupplier<>(configuration, extractKeyFn, extractValueFn);
        } else {
            return new WriteHdfsOldApiP.MetaSupplier<>((JobConf) configuration, extractKeyFn, extractValueFn);
        }
    }
}
