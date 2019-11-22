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
 * A simple example adapted to read from and write to Google Cloud Storage
 * using HDFS source and sink. The example uses Apache Avro input and output
 * format.
 * <p>
 * The job reads records from the given bucket {@link #BUCKET_NAME}, filters
 * according to a field of the record and writes back the records to a folder
 * inside that bucket.
 * <p>
 * To be able to read from and write to Google Cloud Storage, HDFS needs couple
 * of dependencies and the json key file for GCS. Necessary dependencies:
 * <ul>
 *     <li>gcs-connector</li>
 *     <li>guava</li>
 * </ul>
 *
 * @see <a href="https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage">
 * Google Cloud Storage Connector</a> for more information
 */
public class GoogleCloudStorageAvro {

    private static final String JSON_KEY_FILE = "path-to-the-json-key-file";

    private static final String BUCKET_NAME = "jet-gcs-hdfs-avro-bucket";

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("gs://" + BUCKET_NAME + "/");
        Path outputPath = new Path("gs://" + BUCKET_NAME + "/results");

        Configuration configuration = HadoopAvro.createJobConfig(inputPath, outputPath);
        configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        configuration.set("fs.gs.auth.service.account.enable", "true");
        configuration.set("fs.gs.auth.service.account.json.keyfile", JSON_KEY_FILE);
        HadoopAvro.executeSample(configuration);
    }
}
