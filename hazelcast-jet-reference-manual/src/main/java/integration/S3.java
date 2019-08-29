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

package integration;

import com.amazonaws.regions.Regions;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.hadoop.HdfsSinks;
import com.hazelcast.jet.hadoop.HdfsSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.s3.S3Sinks;
import com.hazelcast.jet.s3.S3Sources;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class S3 {

    static void s1() {
        //tag::s1[]
        String accessKeyId = "";
        String accessKeySecret = "";
        String prefix = "";

        Pipeline p = Pipeline.create();
        p.drawFrom(S3Sources.s3("input-bucket", prefix,
                accessKeyId, accessKeySecret, Regions.US_EAST_1))
         .drainTo(Sinks.logger());
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        String accessKeyId = "";
        String accessKeySecret = "";

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list("input-list"))
         .drainTo(S3Sinks.s3("output-bucket", accessKeyId,
                 accessKeySecret, Regions.US_EAST_1));
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        JobConf jobConfig = new JobConf();
        jobConfig.setInputFormat(TextInputFormat.class);
        jobConfig.setOutputFormat(TextOutputFormat.class);
        TextInputFormat.addInputPath(jobConfig, new Path("s3a://input-bucket"));
        TextOutputFormat.setOutputPath(jobConfig, new Path("s3a://output-bucket"));



        Pipeline p = Pipeline.create();
        p.drawFrom(HdfsSources.<String, String>hdfs(jobConfig))
         .map(e -> Util.entry(e.getKey(), e.getValue().toUpperCase()))
         .drainTo(HdfsSinks.hdfs(jobConfig));
        //end::s3[]
    }
}
