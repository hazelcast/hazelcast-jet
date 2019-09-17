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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.hadoop.HdfsSinks;
import com.hazelcast.jet.hadoop.HdfsSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(ParallelTest.class)
public class WriteHdfsPTest extends HdfsTestSupport {

    @Parameterized.Parameter(0)
    public Class outputFormatClass;

    @Parameterized.Parameter(1)
    public Class inputFormatClass;

    @Parameterized.Parameters(name = "Executing: {0} {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[] {TextOutputFormat.class, TextInputFormat.class},
                new Object[] {LazyOutputFormat.class, TextInputFormat.class},
                new Object[] {SequenceFileOutputFormat.class, SequenceFileInputFormat.class},
                new Object[] {org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class},
                new Object[] {org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class},
                new Object[] {org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class}
        );
    }

    @Test
    public void testWriteFile() throws Exception {
        int messageCount = 320;
        String mapName = randomMapName();
        JetInstance instance = createJetMember();
        createJetMember();

        Map<IntWritable, IntWritable> map = IntStream.range(0, messageCount).boxed()
                                                     .collect(toMap(IntWritable::new, IntWritable::new));
        instance.getMap(mapName).putAll(map);

        Path path = getPath();

        JobConf conf = getSinkConf(path);


        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(mapName))
         .drainTo(HdfsSinks.hdfs(conf))
         // we use higher value to increase the race chance for LazyOutputFormat
         .setLocalParallelism(8);

        Future<Void> future = instance.newJob(p).getFuture();
        assertCompletesEventually(future);


        JobConf readJobConf = getReadJobConf(path);

        p = Pipeline.create();
        p.drawFrom(HdfsSources.hdfs(readJobConf))
         .drainTo(Sinks.list("results"));

        future = instance.newJob(p).getFuture();
        assertCompletesEventually(future);


        IList<Object> results = instance.getList("results");
        assertEquals(messageCount, results.size());
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
        JobConf conf = new JobConf();
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

    private Path getPath() throws IOException {
        String dirName = Files.createTempDirectory(getClass().getName()).toString();
        return new Path(dirName);
    }

}
