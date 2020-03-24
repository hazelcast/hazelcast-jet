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

package com.hazelcast.jet.core;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class JobSerializerTest extends SimpleTestInClusterSupport {

    private static final String MAP_NAME = "map";
    private static final String CACHE_NAME = "cache";
    private static final String OBSERVABLE_NAME = "observable";

    @BeforeClass
    public static void beforeClass() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig()
              .addCacheConfig(new CacheSimpleConfig().setName(CACHE_NAME))
              .getSerializationConfig()
              .addSerializerConfig(new SerializerConfig().setTypeClass(Value.class).setClass(ValueSerializer.class));
        initializeWithClient(1, config, null);
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalMapSourceAndSink() {
        // Given
        Pipeline sinkPipeline = Pipeline.create();
        sinkPipeline.readFrom(TestSources.items(13))
                    .map(i -> new SimpleEntry<>(i, new Value(i)))
                    .writeTo(Sinks.map(MAP_NAME));

        // When
        // Then
        assertJobStatusEventually(instance().newJob(sinkPipeline, jobConfig()), COMPLETED);

        // Given
        Pipeline sourcePipeline = Pipeline.create();
        sourcePipeline.readFrom(Sources.<Integer, Value>map(MAP_NAME))
                      .map(Entry::getValue)
                      .writeTo(Sinks.logger());

        // When
        // Then
        assertJobStatusEventually(instance().newJob(sourcePipeline, jobConfig()), COMPLETED);
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalCacheSourceAndSink() {
        // Given
        Pipeline sinkPipeline = Pipeline.create();
        sinkPipeline.readFrom(TestSources.items(13))
                    .map(i -> new SimpleEntry<>(i, new Value(i)))
                    .writeTo(Sinks.cache(CACHE_NAME));

        // When
        // Then
        assertJobStatusEventually(instance().newJob(sinkPipeline, jobConfig()), COMPLETED);

        // Given
        Pipeline sourcePipeline = Pipeline.create();
        sourcePipeline.readFrom(Sources.<Integer, Value>cache(CACHE_NAME))
                      .map(Entry::getValue)
                      .writeTo(Sinks.logger());

        // When
        // Then
        assertJobStatusEventually(instance().newJob(sourcePipeline, jobConfig()), COMPLETED);
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalObservableSink() throws Exception {
        // Given
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1, 2))
                .map(Value::new)
                .writeTo(Sinks.observable(OBSERVABLE_NAME));

        // When
        Observable<Value> observable = instance().getObservable(OBSERVABLE_NAME);
        CompletableFuture<Integer> summer = observable
                .toFuture(values -> values.map(Value::value).reduce(0, Integer::sum));

        // Then
        assertJobStatusEventually(instance().newJob(pipeline, jobConfig()), COMPLETED);
        assertEquals(3, summer.get(5, TimeUnit.SECONDS).intValue());
    }

    private static JobConfig jobConfig() {
        return new JobConfig().registerSerializer(Value.class, ValueSerializer.class);
    }

    private static final class Value {

        private final int value;

        private Value(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }

    private static class ValueSerializer implements StreamSerializer<Value> {

        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void write(ObjectDataOutput output, Value value) throws IOException {
            output.writeInt(value.value);
        }

        @Override
        public Value read(ObjectDataInput input) throws IOException {
            return new Value(input.readInt());
        }
    }
}
