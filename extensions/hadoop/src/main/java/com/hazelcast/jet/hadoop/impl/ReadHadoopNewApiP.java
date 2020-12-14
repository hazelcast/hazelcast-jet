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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.file.impl.FileProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.file.impl.FileTraverser;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.hadoop.HadoopSources.COPY_ON_READ;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * See {@link HadoopSources#inputFormat}.
 */
public final class ReadHadoopNewApiP<K, V, R> extends AbstractProcessor {

    private static final Class<?>[] EMPTY_ARRAY = new Class[0];

    private final Configuration configuration;
    private final List<InputSplit> splits;
    private final BiFunctionEx<K, V, R> projectionFn;

    private HadoopFileTraverser<K, V, R> traverser;

    private ReadHadoopNewApiP(
            @Nonnull Configuration configuration,
            @Nonnull List<InputSplit> splits,
            @Nonnull BiFunctionEx<K, V, R> projectionFn
    ) {
        this.configuration = configuration;
        this.splits = splits;
        this.projectionFn = projectionFn;
    }

    @Override
    protected void init(@Nonnull Context context) {
        InternalSerializationService serializationService = ((ProcCtx) context).serializationService();

        // we clone the projection of key/value if configured so because some of the
        // record-readers return the same object for `reader.getCurrentKey()`
        // and `reader.getCurrentValue()` which is mutated for each `reader.nextKeyValue()`.
        BiFunctionEx<K, V, R> projectionFn = this.projectionFn;
        if (configuration.getBoolean(COPY_ON_READ, true)) {
            BiFunctionEx<K, V, R> actualProjectionFn = projectionFn;
            projectionFn = (key, value) -> {
                R result = actualProjectionFn.apply(key, value);
                return result == null ? null : serializationService.toObject(serializationService.toData(result));
            };
        }

        traverser = new HadoopFileTraverser<>(configuration, splits, projectionFn);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    @Override
    public void close() throws Exception {
        if (traverser != null) {
            traverser.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> InputFormat<K, V> extractInputFormat(Configuration configuration) throws Exception {
        Class<?> inputFormatClass = configuration.getClass(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, TextInputFormat.class);
        Constructor<?> constructor = inputFormatClass.getDeclaredConstructor(EMPTY_ARRAY);
        constructor.setAccessible(true);

        InputFormat<K, V> inputFormat = (InputFormat<K, V>) constructor.newInstance();
        ReflectionUtils.setConf(inputFormat, configuration);
        return inputFormat;
    }

    public static class MetaSupplier<K, V, R> extends ReadHdfsMetaSupplierBase implements FileProcessorMetaSupplier<R> {

        static final long serialVersionUID = 1L;

        /**
         * The instance is either {@link SerializableConfiguration} or {@link
         * SerializableJobConf}, which are serializable.
         */
        @SuppressFBWarnings("SE_BAD_FIELD")
        private final Configuration configuration;
        private final BiFunctionEx<K, V, R> projectionFn;

        private transient Map<Address, List<IndexedInputSplit>> assigned;

        public MetaSupplier(@Nonnull Configuration configuration, @Nonnull BiFunctionEx<K, V, R> projectionFn) {
            this.configuration = configuration;
            this.projectionFn = projectionFn;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            super.init(context);

            List<InputSplit> splits = getSplits(configuration);
            IndexedInputSplit[] indexedInputSplits = new IndexedInputSplit[splits.size()];
            Arrays.setAll(indexedInputSplits, i -> new IndexedInputSplit(i, splits.get(i)));
            Address[] addresses = context.jetInstance().getCluster().getMembers()
                                         .stream()
                                         .map(Member::getAddress)
                                         .toArray(Address[]::new);
            assigned = assignSplitsToMembers(indexedInputSplits, addresses);
            printAssignments(assigned);
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(configuration, assigned.getOrDefault(address, emptyList()), projectionFn);
        }

        @Override
        public FileTraverser<R> traverser() throws Exception {
            return new HadoopFileTraverser<>(configuration, getSplits(configuration), projectionFn);
        }

        private List<InputSplit> getSplits(Configuration configuration) throws Exception {
            InputFormat<K, V> inputFormat = extractInputFormat(configuration);
            Job job = Job.getInstance(configuration);
            try {
                return inputFormat.getSplits(job);
            } catch (InvalidInputException e) {
                String directory = job.getConfiguration().get(INPUT_DIR, "");
                logger.fine("The directory " + directory + " does not exists. This source will emit 0 items.");
                return emptyList();
            }
        }
    }

    private static final class Supplier<K, V, R> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        /**
         * The instance is either {@link SerializableConfiguration} or {@link
         * SerializableJobConf}, which are serializable.
         */
        @SuppressFBWarnings("SE_BAD_FIELD")
        private final Configuration configuration;
        private final BiFunctionEx<K, V, R> projectionFn;
        private final List<IndexedInputSplit> assignedSplits;

        private Supplier(
                @Nonnull Configuration configuration,
                @Nonnull List<IndexedInputSplit> assignedSplits,
                @Nonnull BiFunctionEx<K, V, R> projectionFn
        ) {
            this.configuration = configuration;
            this.projectionFn = projectionFn;
            this.assignedSplits = assignedSplits;
        }

        @Nonnull
        @Override
        public List<Processor> get(int count) {
            Map<Integer, List<IndexedInputSplit>> processorToSplits = Util.distributeObjects(count, assignedSplits);

            return processorToSplits
                    .values().stream()
                    .map(splits -> {
                                List<InputSplit> mappedSplits = splits
                                        .stream()
                                        .map(IndexedInputSplit::getNewSplit)
                                        .collect(toList());
                                return new ReadHadoopNewApiP<>(configuration, mappedSplits, projectionFn);
                            }
                    ).collect(toList());
        }
    }

    private static final class HadoopFileTraverser<K, V, R> implements FileTraverser<R> {

        private final Configuration configuration;
        private final InputFormat<K, V> inputFormat;
        private final BiFunctionEx<K, V, R> projectionFn;
        private final Traverser<R> delegate;

        private RecordReader<K, V> reader;

        private HadoopFileTraverser(
                Configuration configuration,
                List<InputSplit> splits,
                BiFunctionEx<K, V, R> projectionFn
        ) {
            this.configuration = configuration;
            this.inputFormat = uncheckCall(() -> extractInputFormat(configuration));
            this.projectionFn = projectionFn;
            this.delegate = traverseIterable(splits).flatMap(this::traverseSplit);
        }

        private Traverser<R> traverseSplit(InputSplit split) {
            try {
                TaskAttemptContextImpl attemptContext = new TaskAttemptContextImpl(configuration, new TaskAttemptID());
                reader = inputFormat.createRecordReader(split, attemptContext);
                reader.initialize(split, attemptContext);
            } catch (IOException | InterruptedException e) {
                throw sneakyThrow(e);
            }

            return () -> {
                try {
                    while (reader.nextKeyValue()) {
                        R projectedRecord = projectionFn.apply(reader.getCurrentKey(), reader.getCurrentValue());
                        if (projectedRecord != null) {
                            return projectedRecord;
                        }
                    }
                    reader.close();
                    return null;
                } catch (Exception e) {
                    throw sneakyThrow(e);
                }
            };
        }

        @Override
        public R next() {
            return delegate.next();
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
