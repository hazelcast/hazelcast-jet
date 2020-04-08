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

package com.hazelcast.jet.cdc.mongodb;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.JetEvent;
import org.junit.Assert;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AbstractIntegrationTest extends JetTestSupport {

    @Nonnull
    protected static ConsumerEx<List<String>> assertListFn(String... expected) {
        return actualList -> {
            List<String> sortedActualList = new ArrayList<>(actualList);
            sortedActualList.sort(String::compareTo);

            List<String> sortedExpectedList = Arrays.asList(expected);
            sortedExpectedList.sort(String::compareTo);

            Assert.assertEquals(sortedExpectedList, sortedActualList);
        };
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    protected static ProcessorMetaSupplier filterTimestampsProcessorSupplier() {
        /* Trying to make sure that items on the stream have native
         * timestamps. All events should be processed in a short amount
         * of time by Jet, so there is no reason why the difference
         * between their event times and the current time on processing
         * should be significantly different. It is a hack, but it does
         * help detect cases when we don't set useful timestamps at all.*/
        SupplierEx<Processor> supplierEx = Processors.filterP(o -> {
            long timestamp = ((JetEvent<Integer>) o).timestamp();
            long diff = System.currentTimeMillis() - timestamp;
            return diff < TimeUnit.SECONDS.toMillis(3);
        });
        return ProcessorMetaSupplier.preferLocalParallelismOne(supplierEx);
    }

}
