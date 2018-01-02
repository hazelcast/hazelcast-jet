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

package com.hazelcast.jet.datamodel;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.jet.datamodel.BagsByTag.bagsByTag;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.datamodel.Tag.tag0;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static com.hazelcast.jet.datamodel.Tag.tag2;
import static com.hazelcast.jet.datamodel.ThreeBags.threeBags;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.datamodel.TwoBags.twoBags;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;

@RunWith(Parameterized.class)
@Category(ParallelTest.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class DataModelSerializerHooksTest {

    @Parameter
    public Object instance;

    @Parameters
    public static Collection<Object> data() throws Exception {
        return asList(
                new TimestampedEntry<>(1, "key", "value"),
                tuple2("value-0", "value-1"),
                tuple3("value-0", "value-1", "value-2"),
                twoBags(asList("v1", "v2"), asList("v3", "v4")),
                threeBags(asList("v1", "v2"), asList("v3", "v4"), asList("v5", "v6")),
                tag0(),
                tag1(),
                tag2(),
                tag(0),
                tag(1),
                tag(2),
                tag(3),
                itemsByTag(tag0(), "val0",
                        tag1(), "val1",
                        tag2(), null),
                bagsByTag(tag0(), asList("bagv0", "bagv1"),
                        tag1(), asList("bagv2", "bagv3"))
        );
    }

    @Test
    public void testSerializerHook() throws Exception {
        if (!(instance instanceof Map.Entry || instance instanceof Tag)) {
            assertFalse(instance.getClass() + " implements java.io.Serializable", instance instanceof Serializable);
        }
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);
        if (!(instance instanceof Tag)) {
            assertNotSame("serialization/deserialization didn't take place", instance, deserialized);
        }
        assertEquals("objects are not equal after serialize/deserialize", instance, deserialized);
    }
}
