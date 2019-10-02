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

import com.hazelcast.core.IList;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.hadoop.HdfsSources;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class ReadHdfsPTest extends HdfsTestSupport {

    private static final String[] ENTRIES = {
            "key-0 value-0\n",
            "key-1 value-1\n",
            "key-2 value-2\n",
            "key-3 value-3\n"};

    @Parameterized.Parameter
    public Class inputFormatClass;

    @Parameterized.Parameter(1)
    public EMapperType projectionType;

    private Configuration jobConf;
    private Set<Path> paths = new HashSet<>();

    @Parameterized.Parameters(name = "inputFormat={0}, mapper={1}")
    public static Collection<Object[]> parameters() {
        return combinations(
                Arrays.asList(
                        org.apache.hadoop.mapred.TextInputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,
                        org.apache.hadoop.mapred.SequenceFileInputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class),
                Arrays.asList(EMapperType.values()));
    }

    /**
     * Returns all possible combinations that contain one item from each of the
     * given {@code lists}.
     */
    private static Collection<Object[]> combinations(List<?>... lists) {
        Stream<Object[]> stream = Stream.<Object[]>of(new Object[0]);
        for (int i = 0; i < lists.length; i++) {
            int finalI = i;
            stream = stream.flatMap(tuple -> lists[finalI].stream()
                    .map(item -> {
                        Object[] res = Arrays.copyOf(tuple, tuple.length + 1);
                        res[tuple.length] = item;
                        return res;
                    }));
        }
        return stream.collect(toList());
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Before
    public void setup() throws IOException {
        createInputFiles();
        createConfiguration();
    }

    private void createConfiguration() throws IOException {
        if (inputFormatClass.getPackage().getName().contains("mapreduce")) {
            Job job = Job.getInstance();
            job.setInputFormatClass(inputFormatClass);
            for (Path path : paths) {
                org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, path);
            }
            jobConf = job.getConfiguration();
        } else {
            JobConf jobConf = new JobConf();
            this.jobConf = jobConf;
            jobConf.setInputFormat(inputFormatClass);
            for (Path path : paths) {
                org.apache.hadoop.mapred.FileInputFormat.addInputPath(jobConf, path);
            }
        }
    }

    @Test
    public void testReadHdfs() {
        IList<Object> sinkList = instance().getList(randomName());
        Pipeline p = Pipeline.create();
        p.drawFrom(HdfsSources.hdfs(jobConf, projectionType.mapper))
         .setLocalParallelism(4)
         .drainTo(Sinks.list(sinkList))
         .setLocalParallelism(1);

        instance().newJob(p).join();

        assertEquals(projectionType == EMapperType.CUSTOM_WITH_NULLS ? 8 : 16,
                sinkList.size());
        assertTrue(sinkList.get(0).toString().contains("value"));
    }

    private void createInputFiles() throws IOException {
        Configuration conf = new Configuration();
        LocalFileSystem local = FileSystem.getLocal(conf);

        for (int i = 0; i < 4; i++) {
            Path path = createPath();
            paths.add(path);
            if (inputFormatClass.getSimpleName().equals("SequenceFileInputFormat")) {
                createInputSequenceFiles(conf, path);
            } else {
                createInputTextFiles(local, path);
            }
        }
    }

    private static void createInputTextFiles(LocalFileSystem local, Path path) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(local.create(path)))) {
            for (String value : ENTRIES) {
                writer.write(value);
                writer.flush();
            }
        }
    }

    private static void createInputSequenceFiles(Configuration conf, Path path) throws IOException {
        IntWritable key = new IntWritable();
        Text value = new Text();
        Option fileOption = Writer.file(path);
        Option keyClassOption = Writer.keyClass(key.getClass());
        Option valueClassOption = Writer.valueClass(value.getClass());
        try (Writer writer = SequenceFile.createWriter(conf, fileOption, keyClassOption, valueClassOption)) {
            for (int i = 0; i < ENTRIES.length; i++) {
                key.set(i);
                value.set(ENTRIES[i]);
                writer.append(key, value);
            }
        }
    }

    private Path createPath() {
        try {
            String fileName = Files.createTempFile(getClass().getName(), null).toString();
            return new Path(fileName);
        } catch (IOException e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    private enum EMapperType {
        DEFAULT(Util::entry),
        CUSTOM((k, v) -> v.toString()),
        CUSTOM_WITH_NULLS((k, v) -> parseInt(v.toString().substring(4, 5)) % 2 == 0 ? v.toString() : null);

        private final BiFunctionEx<?, Text, ?> mapper;

        EMapperType(BiFunctionEx<?, Text, ?> mapper) {
            this.mapper = mapper;
        }
    }
}
