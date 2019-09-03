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

package com.hazelcast.jet.examples.s3;

import com.amazonaws.regions.Regions;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.s3.S3Parameters;
import com.hazelcast.jet.s3.S3Sinks;
import com.hazelcast.jet.s3.S3Sources;

import java.util.regex.Pattern;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.Functions.wholeItem;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Word count example adapted to read from and write to S3 bucket.
 * <p>
 * For more details about the word count pipeline itself, please see the JavaDoc
 * for the {@code WordCount} class in {@code wordcount} sample.
 * <p>
 * {@link S3Sources#s3(String, String, S3Parameters)}
 * is a source that can be used for reading from an S3 bucket with the given
 * credentials. The files in the input bucket will be split among Jet
 * processors.
 * <p>
 * {@link S3Sinks#s3(String, S3Parameters)}
 * writes the output to the given output bucket, with each
 * processor writing to a batch of files within the bucket. The files are
 * identified by the global processor index and an incremented value.
 */
public class S3WordCount {

    private static Pipeline buildPipeline(
            String secretAccessKey, String accessKeySecret, Regions region,
            String inputBucket, String outputBucket
    ) {
        S3Parameters s3Parameters = S3Parameters.create(secretAccessKey, accessKeySecret, region);
        final Pattern regex = Pattern.compile("\\W+");
        Pipeline p = Pipeline.create();
        p.drawFrom(S3Sources.s3(inputBucket, null, s3Parameters))
         .flatMap(line -> traverseArray(regex.split(line.toLowerCase())).filter(w -> !w.isEmpty()))
         .groupingKey(wholeItem())
         .aggregate(counting())
         .drainTo(S3Sinks.s3(outputBucket, s3Parameters));
        return p;
    }

    public static void main(String[] args) {
        String secretAccessKey = "";
        String accessKeySecret = "";
        Regions region = Regions.US_EAST_1;

        String inputBucket = "";
        String outputBucket = "";
        try {
            JetInstance jet = Jet.newJetInstance();
            Jet.newJetInstance();
            System.out.print("\nCounting words from " + inputBucket);
            long start = nanoTime();
            Pipeline p = buildPipeline(secretAccessKey, accessKeySecret, region, inputBucket, outputBucket);
            jet.newJob(p).join();
            System.out.println("Done in " + NANOSECONDS.toMillis(nanoTime() - start) + " milliseconds.");
            System.out.println("Output written to " + outputBucket);
        } finally {
            Jet.shutdownAll();
        }
    }
}
