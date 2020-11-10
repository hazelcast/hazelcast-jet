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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.processor.TransformBatchedP;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.connector.map.JoinProcessors.JoinProcessorFactory;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.extract.QueryPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

final class JoinScanProcessorFactory implements JoinProcessorFactory {

    static final JoinScanProcessorFactory INSTANCE = new JoinScanProcessorFactory();

    private static final int MAX_BATCH_SIZE = 1024;

    private JoinScanProcessorFactory() {
    }

    @Override
    public Processor create(
            ServiceFactory<Object, IMap<Object, Object>> mapFactory,
            IMap<Object, Object> map,
            QueryPath[] rightPaths,
            SupplierEx<KvRowProjector> rightProjectorSupplier,
            JoinInfo joinInfo
    ) {
        return new TransformBatchedP<Object[], Object[]>(
                MAX_BATCH_SIZE,
                joinFn(map, rightProjectorSupplier.get(), joinInfo)
        ) {
            @Override
            public boolean isCooperative() {
                return false;
            }
        };
    }

    private static FunctionEx<List<? super Object[]>, Traverser<Object[]>> joinFn(
            IMap<Object, Object> map,
            KvRowProjector rightProjector,
            JoinInfo joinInfo
    ) {
        BiFunctionEx<Object[], Object[], Object[]> joinFn = ExpressionUtil.joinFn(joinInfo.condition());

        return lefts -> {
            List<Object[]> rows = new ArrayList<>();
            for (Entry<Object, Object> entry : map.entrySet()) {
                Object[] right = rightProjector.project(entry);
                if (right == null) {
                    continue;
                }

                for (Object left : lefts) {
                    Object[] joined = joinFn.apply((Object[]) left, right);
                    if (joined != null) {
                        rows.add(joined);
                    }
                }
            }
            return Traversers.traverseIterable(rows);
        };
    }
}
