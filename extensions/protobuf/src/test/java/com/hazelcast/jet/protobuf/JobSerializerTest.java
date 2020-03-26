/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.protobuf;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.protobuf.Messages.Person;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
public class JobSerializerTest extends SimpleTestInClusterSupport {

    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(1, new JetConfig(), new JetClientConfig());
    }

    @Test
    public void when_serializerIsNotRegistered_then_throwsException() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(85))
                .map(value -> new SimpleEntry<>(value, Person.newBuilder().setName("Joe").setAge(value).build()))
                .writeTo(Sinks.map("map-1"));

        client().newJob(pipeline, jobConfig()).join();

        Map<Integer, Person> map = client().getMap("map-1");
        assertThatThrownBy(() -> map.get(85)).isInstanceOf(HazelcastSerializationException.class);
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForTheJob() {
        Pipeline sinkPipeline = Pipeline.create();
        sinkPipeline.readFrom(TestSources.items(33, 66))
                    .map(value -> new SimpleEntry<>(value, Person.newBuilder().setName("Joe").setAge(value).build()))
                    .writeTo(Sinks.map("map-2"));
        client().newJob(sinkPipeline, jobConfig()).join();

        Pipeline sourcePipeline = Pipeline.create();
        sourcePipeline.readFrom(Sources.<Integer, Person>map("map-2"))
                      .map(entry -> entry.getValue().getAge())
                      .writeTo(AssertionSinks.assertAnyOrder(asList(33, 66)));
        client().newJob(sourcePipeline, jobConfig()).join();
    }

    private static JobConfig jobConfig() {
        return new JobConfig().registerProtobufSerializer(Person.class, 1);
    }
}
