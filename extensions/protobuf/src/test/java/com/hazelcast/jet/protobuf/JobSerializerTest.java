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

import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.protobuf.Messages.Person;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
public class JobSerializerTest extends SimpleTestInClusterSupport {

    private static final int TYPE_ID = 1;

    @BeforeClass
    public static void beforeClass() {
        JetConfig config = new JetConfig();

        JetClientConfig clientConfig = new JetClientConfig();
        clientConfig.getSerializationConfig()
                    .addSerializerConfig(
                            new SerializerConfig()
                                    .setTypeClass(Person.class)
                                    .setImplementation(new ProtoStreamSerializer<>(TYPE_ID, Person.class))
                    );

        initializeWithClient(1, config, clientConfig);
    }

    @Test
    public void when_serializerIsNotRegistered_then_throwsException() {
        String name = "map-1";
        IMap<Integer, Person> map = client().getMap(name);
        map.put(1, Person.newBuilder().setName("Joe").setAge(33).build());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<Integer, Person>map(name))
                .map(entry -> entry.getValue().getName())
                .writeTo(Sinks.logger());

        assertThatThrownBy(() -> client().newJob(pipeline, new JobConfig()).join())
                .hasCauseInstanceOf(JetException.class);
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForTheJob() {
        String name = "map-2";
        IMap<Integer, Person> map = client().getMap(name);
        map.put(1, Person.newBuilder().setName("Joe").setAge(33).build());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<Integer, Person>map(name))
                .map(entry -> entry.getValue().getName())
                .writeTo(AssertionSinks.assertAnyOrder(singletonList("Joe")));

        client().newJob(pipeline, jobConfig()).join();
    }

    private static JobConfig jobConfig() {
        return new JobConfig().registerProtoSerializer(Person.class, TYPE_ID);
    }
}
