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
import com.hazelcast.jet.hadoop.HdfsSinks;
import com.hazelcast.jet.hadoop.HdfsSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class WriteHdfsPTest extends HdfsTestSupport {

    @Parameterized.Parameter
    public Class outputFormatClass;

    @Parameterized.Parameter(1)
    public Class inputFormatClass;

    private java.nio.file.Path javaDir;
    private Path hadoopDir;

    @Parameterized.Parameters(name = "Executing: {0} {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[] {TextOutputFormat.class, TextInputFormat.class},
                new Object[] {LazyOutputFormat.class, TextInputFormat.class},
                new Object[] {SequenceFileOutputFormat.class, SequenceFileInputFormat.class},
                new Object[] {
                        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class},
                new Object[] {
                        org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class},
                new Object[] {
                        org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class}
        );
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Before
    public void before() throws IOException {
        javaDir = Files.createTempDirectory(getClass().getSimpleName());
        hadoopDir = new Path(javaDir.toString());
    }

    @After
    public void after() {
        IOUtil.delete(javaDir.toFile());
    }

    @Test
    public void testWrite_newApi() throws Exception {
        JobConf conf = getSinkConf(hadoopDir);

        int messageCount = 320;
        Pipeline p = Pipeline.create();
        p.drawFrom(TestSources.items(IntStream.range(0, messageCount).boxed().toArray(Integer[]::new)))
         .map(num -> entry(new IntWritable(num), new IntWritable(num)))
         .drainTo(HdfsSinks.hdfs(conf))
         // we use higher value to increase the race chance for LazyOutputFormat
         .setLocalParallelism(8);

        instance().newJob(p).join();
        JobConf readJobConf = getReadJobConf(hadoopDir);

        p = Pipeline.create();
        IList<Entry> resultList = instance().getList(randomName());
        p.drawFrom(HdfsSources.hdfsNewApi(readJobConf))
         .drainTo(Sinks.list(resultList));

        instance().newJob(p).join();
        logger.info("Result list contents:\n" + resultList.stream().map(e -> e.toString() + ", key type: " + e.getKey().getClass().getName() + ", value type: " + e.getValue().getClass().getName()).collect(joining("\n")));
        assertEquals(messageCount, resultList.size());
    }

    private JobConf getReadJobConf(Path path) throws IOException {
        JobConf conf = new JobConf();
        if (inputFormatClass.getPackage().getName().contains("mapreduce")) {
            Job job = Job.getInstance();
            job.setInputFormatClass(inputFormatClass);
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, path);
            conf = new JobConf(job.getConfiguration());
        } else {
            conf.setInputFormat(inputFormatClass);
            FileInputFormat.addInputPath(conf, path);
        }
        return conf;
    }

    private JobConf getSinkConf(Path path) throws IOException {
        JobConf conf;
        if (outputFormatClass.getPackage().getName().contains("mapreduce")) {
            Job job = Job.getInstance();
            job.setOutputFormatClass(outputFormatClass);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);
            org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, path);
            if (outputFormatClass.equals(org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat.class)) {
                org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat.setOutputFormatClass(job,
                        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
            }
            conf = new JobConf(job.getConfiguration());
        } else {
            conf = new JobConf();
            conf.setOutputFormat(outputFormatClass);
            conf.setOutputCommitter(FileOutputCommitter.class);
            conf.setOutputKeyClass(IntWritable.class);
            conf.setOutputValueClass(IntWritable.class);
            FileOutputFormat.setOutputPath(conf, path);
            if (outputFormatClass.equals(LazyOutputFormat.class)) {
                LazyOutputFormat.setOutputFormatClass(conf, TextOutputFormat.class);
            }
        }
        return conf;
    }
}
