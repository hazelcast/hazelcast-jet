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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.processor.AsyncTransformUsingServiceOrderedP;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.connector.map.JoinProcessors.JoinProcessorFactory;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.extract.QueryPath;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.entry;

final class JoinByPrimitiveKeyProcessorFactory implements JoinProcessorFactory {

    static final JoinByPrimitiveKeyProcessorFactory INSTANCE = new JoinByPrimitiveKeyProcessorFactory();

    private static final int MAX_CONCURRENT_OPS = 8;

    private JoinByPrimitiveKeyProcessorFactory() {
    }

    @Override
    public Processor create(
            ServiceFactory<Object, IMap<Object, Object>> mapFactory,
            IMap<Object, Object> map,
            QueryPath[] rightPaths,
            SupplierEx<KvRowProjector> rightProjectorSupplier,
            JetJoinInfo jetJoinInfo
    ) {
        return new AsyncTransformUsingServiceOrderedP<>(
                mapFactory,
                map,
                MAX_CONCURRENT_OPS,
                callAsyncFn(rightProjectorSupplier, jetJoinInfo)
        );
    }

    private static BiFunctionEx<IMap<Object, Object>, Object[], CompletableFuture<Traverser<Object[]>>> callAsyncFn(
            SupplierEx<KvRowProjector> rightProjectorSupplier,
            JetJoinInfo jetJoinInfo
    ) {
        assert jetJoinInfo.leftEquiJoinIndices().length == 1;

        int leftEquiJoinIndex = jetJoinInfo.leftEquiJoinIndices()[0];
        BiFunctionEx<Object[], Object[], Object[]> joinFn = ExpressionUtil.joinFn(jetJoinInfo.nonEquiCondition());

        return (map, left) -> {
            Object key = left[leftEquiJoinIndex];
            if (key == null) {
                return null;
            }

            return map.getAsync(key).toCompletableFuture()
                      .thenApply(value -> {
                          if (value == null) {
                              return null;
                          }

                          Object[] right = rightProjectorSupplier.get().project(entry(key, value));
                          if (right == null) {
                              return null;
                          }

                          Object[] joined = joinFn.apply(left, right);
                          if (joined != null) {
                              return Traversers.singleton(joined);
                          }
                          return null;
                      });
        };
    }
}
