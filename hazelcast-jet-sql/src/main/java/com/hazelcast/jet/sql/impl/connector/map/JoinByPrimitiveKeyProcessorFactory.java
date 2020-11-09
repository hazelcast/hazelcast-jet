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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.connector.map.JoinProcessors.JoinProcessorFactory;
import com.hazelcast.jet.sql.impl.join.JoinInfo;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.extract.QueryPath;

import static com.hazelcast.jet.Util.entry;

final class JoinByPrimitiveKeyProcessorFactory implements JoinProcessorFactory {

    static final JoinByPrimitiveKeyProcessorFactory INSTANCE = new JoinByPrimitiveKeyProcessorFactory();

    private JoinByPrimitiveKeyProcessorFactory() {
    }

    @Override
    public Processor create(
            IMap<Object, Object> map,
            QueryPath[] rightPaths,
            KvRowProjector rightProjector,
            JoinInfo joinInfo
    ) {
        return new TransformP<>(joinFn(map, rightProjector, joinInfo));
    }

    private static FunctionEx<Object[], Traverser<Object[]>> joinFn(
            IMap<Object, Object> map,
            KvRowProjector rightProjector,
            JoinInfo joinInfo
    ) {
        assert joinInfo.leftEquiJoinIndices().length == 1;

        int leftEquiJoinIndex = joinInfo.leftEquiJoinIndices()[0];
        BiFunctionEx<Object[], Object[], Object[]> joinFn = ExpressionUtil.joinFn(joinInfo.nonEquiCondition());

        ResettableSingletonTraverser<Object[]> traverser = new ResettableSingletonTraverser<>();
        return left -> {
            Object key = left[leftEquiJoinIndex];
            if (key == null) {
                return traverser;
            }

            Object value = map.get(key);
            if (value == null) {
                return traverser;
            }

            Object[] right = rightProjector.project(entry(key, value));
            if (right == null) {
                return traverser;
            }

            Object[] joined = joinFn.apply(left, right);
            if (joined != null) {
                traverser.accept(joined);
            }
            return traverser;
        };
    }
}
