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

package com.hazelcast.jet.cdc;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractCdcIntegrationTest extends JetTestSupport {

    @Nonnull
    protected static List<String> mapResultsToSortedList(IMap<?, ?> map) {
        return map.entrySet().stream()
                .map(e -> e.getKey() + ":" + e.getValue())
                .sorted().collect(Collectors.toList());
    }

    @Nonnull
    protected static void assertMatch(List<String> expectedPatterns, List<String> actualValues) {
        assertEquals(expectedPatterns.size(), actualValues.size());
        for (int i = 0; i < expectedPatterns.size(); i++) {
            String pattern = expectedPatterns.get(i);
            String value = actualValues.get(i);
            assertTrue(value.matches(pattern));
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    protected static ProcessorMetaSupplier filterTimestampsProcessorSupplier() {
        /* Trying to make sure that items on the stream have native
         * timestamps. All records should be processed in a short amount
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

    protected static class TableRow {

        @JsonProperty("id")
        public int id;

        @JsonProperty("value_1")
        public String value1;

        @JsonProperty("value_2")
        public String value2;

        @JsonProperty("value_3")
        public String value3;

        TableRow() {
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, value1, value2, value3);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TableRow other = (TableRow) obj;
            return id == other.id
                    && Objects.equals(value1, other.value1)
                    && Objects.equals(value2, other.value2)
                    && Objects.equals(value3, other.value3);
        }

        @Override
        public String toString() {
            return "TableRow {id=" + id + ", value1=" + value1 + ", value2=" + value2 + ", value3=" + value3 + '}';
        }
    }

}
