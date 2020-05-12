/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.elastic;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.elastic.ElasticSinks;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import org.elasticsearch.action.index.IndexRequest;

import java.io.File;
import java.nio.file.Files;
import java.util.stream.Stream;

import static com.hazelcast.jet.pipeline.Pipeline.create;
import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ElasticSinkExample {

    public static void main(String[] args) {
        try {
            Pipeline p = create();
            p.readFrom(files("src/main/resources/documents"))
             .map(JsonUtil::parse)
             .writeTo(ElasticSinks.elastic(map ->
                     new IndexRequest("my-index")
                             .source(map)
                             .version((Long) map.get("version"))
             ));

            JetInstance jet = Jet.newJetInstance();
            jet.newJob(p).join();
        } catch (Exception e) {
            Jet.shutdownAll();
        }
    }

    public static BatchSource<String> files(String directory) {
        return batchFromProcessor("filesSource(" + new File(directory) + ')',
                SourceProcessors.readFilesP(directory, "*", false,
                        path -> Stream.of(new String(Files.readAllBytes(path), UTF_8))));
    }

}
