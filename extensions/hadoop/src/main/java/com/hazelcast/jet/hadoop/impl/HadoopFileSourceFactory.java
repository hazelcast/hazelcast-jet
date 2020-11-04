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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.file.AvroFileFormat;
import com.hazelcast.jet.pipeline.file.CsvFileFormat;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.JsonFileFormat;
import com.hazelcast.jet.pipeline.file.LinesTextFileFormat;
import com.hazelcast.jet.pipeline.file.ParquetFileFormat;
import com.hazelcast.jet.pipeline.file.RawBytesFileFormat;
import com.hazelcast.jet.pipeline.file.TextFileFormat;
import com.hazelcast.jet.pipeline.file.impl.FileSourceFactory;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.hadoop.impl.CsvInputFormat.CSV_INPUT_FORMAT_BEAN_CLASS;
import static com.hazelcast.jet.hadoop.impl.JsonInputFormat.JSON_INPUT_FORMAT_BEAN_CLASS;
import static java.util.Objects.requireNonNull;

/**
 * Hadoop-based implementation of {@link FileSourceFactory}.
 */
public class HadoopFileSourceFactory implements FileSourceFactory {

    private final Map<String, JobConfigurer> configs;

    /**
     * Creates the HadoopSourceFactory.
     */
    public HadoopFileSourceFactory() {
        configs = new HashMap<>();

        configs.put(AvroFileFormat.FORMAT_AVRO, new AvroFormatJobConfigurer());
        configs.put(CsvFileFormat.FORMAT_CSV, new CsvFormatJobConfigurer());
        configs.put(JsonFileFormat.FORMAT_JSONL, new JsonFormatJobConfigurer());
        configs.put(LinesTextFileFormat.FORMAT_LINES, new LineTextJobConfigurer());
        configs.put(ParquetFileFormat.FORMAT_PARQUET, new ParquetFormatJobConfigurer());
        configs.put(RawBytesFileFormat.FORMAT_BIN, new RawBytesFormatJobConfigurer());
        configs.put(TextFileFormat.FORMAT_TXT, new TextJobConfigurer());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> BatchSource<T> create(@Nonnull FileSourceBuilder<T> builder) {

        try {
            Job job = Job.getInstance();

            Configuration configuration = job.getConfiguration();
            for (Entry<String, String> option : builder.options().entrySet()) {
                configuration.set(option.getKey(), option.getValue());
            }

            FileInputFormat.addInputPath(job, new Path(builder.path()));

            FileFormat<T> fileFormat = requireNonNull(builder.format());
            JobConfigurer configurer = configs.get(fileFormat.format());
            configurer.configure(job, fileFormat);

            return HadoopSources.inputFormat(configuration, (BiFunctionEx<?, ?, T>) configurer.projectionFn());
        } catch (IOException e) {
            throw new JetException("Could not create a source", e);
        }
    }

    /**
     * Hadoop map-reduce job configurer.
     */
    public interface JobConfigurer {

        /**
         * Configures the given job with the given file format.
         * <p>
         * This method should set the input format class and any required
         * configuration parameters.
         *
         * @param job    map-reduce job to configure
         * @param format format to configure the job with
         */
        <T> void configure(Job job, FileFormat<T> format);

        /**
         * Projection function from the key-value result of the map-reduce job into
         * the item emitted from the source.
         * <p>
         * The types of key/value are determined by the input format class set by
         * the configure method.
         *
         * @return projection function from key-value result into the item emitted from the
         *         source
         */
        BiFunctionEx<?, ?, ?> projectionFn();
    }

    private static class AvroFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            AvroFileFormat<T> avroFileFormat = (AvroFileFormat<T>) format;
            job.setInputFormatClass(AvroKeyInputFormat.class);

            Class<?> reflectClass = avroFileFormat.reflectClass();
            if (reflectClass != null) {
                Schema schema = ReflectData.get().getSchema(reflectClass);
                AvroJob.setInputKeySchema(job, schema);
            }
        }

        @Override
        public BiFunctionEx<AvroKey<?>, NullWritable, ?> projectionFn() {
            return (k, v) -> k.datum();
        }
    }

    private static class RawBytesFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            job.setInputFormatClass(WholeFileInputFormat.class);
        }

        @Override
        public BiFunctionEx<NullWritable, BytesWritable, byte[]> projectionFn() {
            return (k, v) -> v.copyBytes();
        }

    }

    private static class CsvFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            CsvFileFormat<T> csvFileFormat = (CsvFileFormat<T>) format;
            job.setInputFormatClass(CsvInputFormat.class);
            job.getConfiguration().set(CSV_INPUT_FORMAT_BEAN_CLASS, csvFileFormat.clazz().getCanonicalName());
        }

        @Override
        public BiFunctionEx<NullWritable, ?, ?> projectionFn() {
            return (k, v) -> v;
        }
    }

    private static class JsonFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            JsonFileFormat<T> jsonFileFormat = (JsonFileFormat<T>) format;
            job.setInputFormatClass(JsonInputFormat.class);
            job.getConfiguration().set(JSON_INPUT_FORMAT_BEAN_CLASS, jsonFileFormat.clazz().getCanonicalName());
        }

        @Override
        public BiFunctionEx<LongWritable, ?, ?> projectionFn() {
            return (k, v) -> v;
        }
    }

    private static class LineTextJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            job.setInputFormatClass(TextInputFormat.class);
        }

        @Override
        public BiFunctionEx<LongWritable, Text, String> projectionFn() {
            return (k, v) -> v.toString();
        }
    }

    private static class ParquetFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            job.setInputFormatClass(AvroParquetInputFormat.class);
        }

        @Override
        public BiFunctionEx<String, ?, ?> projectionFn() {
            return (k, v) -> v;
        }
    }

    private static class TextJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            job.setInputFormatClass(WholeTextInputFormat.class);
        }

        @Override
        public BiFunctionEx<NullWritable, Text, String> projectionFn() {
            return (k, v) -> v.toString();
        }
    }

}
