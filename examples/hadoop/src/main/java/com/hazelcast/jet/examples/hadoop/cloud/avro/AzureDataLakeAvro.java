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
 * A simple example adapted to read from and write to Azure Data Lake Storage
 * using HDFS source and sink. The example uses Apache Avro input and output
 * format.
 * <p>
 * The job reads records from the given bucket {@link #FOLDER_NAME}, filters
 * according to a field of the record and writes back the records to a folder
 * inside that bucket.
 * <p>
 * To be able to read from and write to Azure Data Lake Storage, HDFS needs
 * {@code hadoop-azure-datalake} as dependency and credentials for the storage
 * account.
 *
 * @see <a href="https://hadoop.apache.org/docs/r3.0.3/hadoop-azure-datalake/index.html">
 * Hadoop Azure Data Lake Support</a> for more information
 */
public class AzureDataLakeAvro {


    private static final String ACCOUNT_NAME = "";
    private static final String CLIENT_ID = "";
    private static final String TENANT_ID = "";
    private static final String CLIENT_CREDENTIALS = "";
    private static final String FOLDER_NAME = "jet-azure-data-lake-avro-folder";

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("adl://" + ACCOUNT_NAME + ".azuredatalakestore.net/" + FOLDER_NAME);
        Path outputPath = new Path("adl://" + ACCOUNT_NAME + ".azuredatalakestore.net/" + FOLDER_NAME + "/results");

        Configuration configuration = HadoopAvro.createJobConfig(inputPath, outputPath);
        configuration.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential");
        configuration.set("fs.adl.oauth2.refresh.url", "https://login.microsoftonline.com/" + TENANT_ID + "/oauth2/token");
        configuration.set("fs.adl.oauth2.client.id", CLIENT_ID);
        configuration.set("fs.adl.oauth2.credential", CLIENT_CREDENTIALS);
        HadoopAvro.executeSample(configuration);
    }
}
