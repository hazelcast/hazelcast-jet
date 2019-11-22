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

package com.hazelcast.jet.examples.hadoop.cloud.avro;

import com.hazelcast.jet.examples.hadoop.HadoopAvro;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A simple example adapted to read from and write to Azure Cloud Storage using
 * HDFS source and sink. The example uses Apache Avro input and output format.
 * <p>
 * The job reads records from the given bucket {@link #CONTAINER_NAME}, filters
 * according to a field of the record and writes back the records to a folder
 * inside that bucket.
 * <p>
 * To be able to read from and write to Azure Cloud Storage, HDFS needs {@code
 * hadoop-azure} as dependency and an access key of a Storage Account.
 *
 * @see <a href="https://hadoop.apache.org/docs/r3.0.3/hadoop-azure/index.html">
 * Hadoop Azure Support</a> for more information
 */
public class AzureCloudStorageAvro {

    private static final String ACCESS_KEY = "";

    private static final String ACCOUNT_NAME = "";
    private static final String CONTAINER_NAME = "jet-azure-hdfs-avro-container";

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("wasbs://" + CONTAINER_NAME + "@" + ACCOUNT_NAME + ".blob.core.windows.net/");
        Path outputPath = new Path("wasbs://" + CONTAINER_NAME + "@" + ACCOUNT_NAME + ".blob.core.windows.net/results");

        Configuration configuration = HadoopAvro.createJobConfig(inputPath, outputPath);
        configuration.set("fs.azure.account.key." + ACCOUNT_NAME + ".blob.core.windows.net", ACCESS_KEY);
        HadoopAvro.executeSample(configuration);
    }
}
