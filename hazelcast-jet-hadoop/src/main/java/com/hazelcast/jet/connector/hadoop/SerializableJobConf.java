/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.connector.hadoop;

import org.apache.hadoop.mapred.JobConf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * This class is used to make {@link JobConf} object serializable
 */
final class SerializableJobConf extends JobConf implements Serializable {

    SerializableJobConf() {
        //For deserialization
    }

    SerializableJobConf(JobConf jobConf) {
        super(jobConf);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        super.write(new DataOutputStream(out));
    }

    private void readObject(ObjectInputStream in) throws IOException {
        super.readFields(new DataInputStream(in));
    }

    static SerializableJobConf asSerializable(JobConf conf) {
        return new SerializableJobConf(conf);
    }
}
