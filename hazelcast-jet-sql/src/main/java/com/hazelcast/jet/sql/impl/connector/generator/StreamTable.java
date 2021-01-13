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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.GeneratorFunction;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;
import java.util.concurrent.TimeUnit;

class StreamTable extends JetTable {

    private final int rate;

    StreamTable(
            SqlConnector sqlConnector,
            List<TableField> fields,
            String schemaName,
            String name,
            int rate
    ) {
        super(sqlConnector, fields, schemaName, name, new ConstantTableStatistics(Integer.MAX_VALUE));

        if (rate < 0) {
            throw QueryException.error("rate cannot be less than zero");
        }

        this.rate = rate;
    }

    StreamSource<Object[]> items(Expression<Boolean> predicate, List<Expression<?>> projections) {
        int rate = this.rate;
        GeneratorFunction<Object[]> generator =
                (timestamp, sequence) -> ExpressionUtil.evaluate(predicate, projections, new Object[]{sequence});
        return SourceBuilder.timestampedStream("itemStream", ctx -> new ItemStreamSource(rate, generator))
                            .fillBufferFn(ItemStreamSource::fillBuffer)
                            .build();
    }

    private static final class ItemStreamSource {

        private static final int MAX_BATCH_SIZE = 1024;

        private final GeneratorFunction<Object[]> generator;
        private final long periodNanos;

        private long emitSchedule;
        private long sequence;

        private ItemStreamSource(int itemsPerSecond, GeneratorFunction<Object[]> generator) {
            this.periodNanos = TimeUnit.SECONDS.toNanos(1) / itemsPerSecond;
            this.generator = generator;
        }

        void fillBuffer(TimestampedSourceBuffer<Object[]> buffer) throws Exception {
            long nowNs = System.nanoTime();
            if (emitSchedule == 0) {
                emitSchedule = nowNs;
            }
            // round ts down to nearest period
            long tsNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
            long ts = TimeUnit.NANOSECONDS.toMillis(tsNanos - (tsNanos % periodNanos));
            for (int i = 0; i < MAX_BATCH_SIZE && nowNs >= emitSchedule; i++) {
                Object[] item = generator.generate(ts, sequence++);
                if (item != null) {
                    buffer.add(item, ts);
                }
                emitSchedule += periodNanos;
            }
        }
    }
}
