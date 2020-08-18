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

package com.hazelcast.jet.hadoop;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.hadoop.impl.ReadHadoopNewApiP;
import com.hazelcast.jet.hadoop.impl.ReadHadoopOldApiP;
import com.hazelcast.jet.hadoop.impl.SerializableConfiguration;
import com.hazelcast.jet.hadoop.impl.WriteHadoopNewApiP;
import com.hazelcast.jet.hadoop.impl.WriteHadoopOldApiP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;

import javax.annotation.Nonnull;
import java.util.function.BiFunction;

/**
 * Static utility class with factories of Apache Hadoop source and sink
 * processors.
 *
 * @since 3.0
 */
public final class HadoopProcessors {

    private HadoopProcessors() {
    }

    /**
     * Returns a supplier of processors for
     * {@link HadoopSources#inputFormat(Configuration, BiFunctionEx)}.
     */
    @Nonnull
    public static <K, V, R> ProcessorMetaSupplier readHadoopP(
            @Nonnull Configuration configuration,
            @Nonnull BiFunctionEx<K, V, R> projectionFn
    ) {
        configuration = SerializableConfiguration.asSerializable(configuration);
        if (configuration.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR) != null) {
            return new ReadHadoopNewApiP.MetaSupplier<>(configuration, () -> projectionFn);
        } else {
            return new ReadHadoopOldApiP.MetaSupplier<>((JobConf) configuration, projectionFn);
        }
    }

    /**
     * @param projectionSupplierFn supplier of projection function. The
     *     supplier will be used to get a projectionFn once for each parallel
     *     processor, therefore the returned function can be stateful and will be
     *     used from a single thread.
     */
    @Nonnull
    public static <K, V, R> ProcessorMetaSupplier readHadoopP(
            @Nonnull Configuration configuration,
            @Nonnull SupplierEx<BiFunction<K, V, R>> projectionSupplierFn
    ) {
        configuration = SerializableConfiguration.asSerializable(configuration);
        if (configuration.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR) != null) {
            return new ReadHadoopNewApiP.MetaSupplier<>(configuration, projectionSupplierFn);
        } else {
            throw new RuntimeException("The old Hadoop API doesn't support projectionSupplierFn");
        }
    }

    /**
     * Returns a supplier of processors for
     * {@link HadoopSinks#outputFormat(Configuration, FunctionEx, FunctionEx)}.
     */
    @Nonnull
    public static <E, K, V> ProcessorMetaSupplier writeHadoopP(
            @Nonnull Configuration configuration,
            @Nonnull FunctionEx<? super E, K> extractKeyFn,
            @Nonnull FunctionEx<? super E, V> extractValueFn
    ) {
        configuration = SerializableConfiguration.asSerializable(configuration);
        if (configuration.get(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR) != null) {
            return new WriteHadoopNewApiP.MetaSupplier<>(configuration, extractKeyFn, extractValueFn);
        } else {
            return new WriteHadoopOldApiP.MetaSupplier<>((JobConf) configuration, extractKeyFn, extractValueFn);
        }
    }
}
