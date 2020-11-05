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
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.connector.map.JoinProcessors.JoinProcessorFactory;
import com.hazelcast.jet.sql.impl.join.JoinInfo;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.sql.impl.extract.QueryPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

final class JoinByPredicateProcessorFactory implements JoinProcessorFactory {

    static final JoinByPredicateProcessorFactory INSTANCE = new JoinByPredicateProcessorFactory();

    private JoinByPredicateProcessorFactory() {
    }

    @Override
    public Processor create(
            IMap<Object, Object> map,
            QueryPath[] rightPaths,
            KvRowProjector rightProjector,
            JoinInfo joinInfo
    ) {
        return new TransformP<>(joinFn(map, rightPaths, rightProjector, joinInfo));
    }

    private static FunctionEx<Object[], Traverser<Object[]>> joinFn(
            IMap<Object, Object> map,
            QueryPath[] rightPaths,
            KvRowProjector rightProjector,
            JoinInfo joinInfo
    ) {
        assert joinInfo.isEquiJoin();

        int[] leftEquiJoinIndices = joinInfo.leftEquiJoinIndices();
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        BiFunctionEx<Object[], Object[], Object[]> joinFn = ExpressionUtil.joinFn(joinInfo.nonEquiCondition());

        return left -> {
            Predicate<Object, Object> predicate = toPredicate(left, leftEquiJoinIndices, rightEquiJoinIndices, rightPaths);
            if (predicate == null) {
                return Traversers.empty();
            }

            List<Object[]> rows = new ArrayList<>();
            for (Entry<Object, Object> entry : map.entrySet(predicate)) {
                Object[] right = rightProjector.project(entry);
                Object[] joined = joinFn.apply(left, right);
                if (joined != null) {
                    rows.add(joined);
                }
            }
            return Traversers.traverseIterable(rows);
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Predicate<Object, Object> toPredicate(
            Object[] left,
            int[] leftEquiJoinIndices,
            int[] rightEquiJoinIndices,
            QueryPath[] rightPaths
    ) {
        PredicateBuilder builder = Predicates.newPredicateBuilder();
        EntryObject entryObject = builder.getEntryObject();
        for (int i = 0; i < leftEquiJoinIndices.length; i++) {
            Comparable leftValue = (Comparable) left[leftEquiJoinIndices[i]];

            // might need a change when/if IS NOT DISTINCT FROM is supported
            if (leftValue == null) {
                return null;
            }

            QueryPath rightPath = rightPaths[rightEquiJoinIndices[i]];

            EntryObject object;
            if (rightPath.isKey()) {
                object = rightPath.isTop()
                        ? entryObject.key()
                        : entryObject.key().get(rightPath.getPath());
            } else {
                object = rightPath.isTop()
                        ? entryObject.get(rightPath.toString())
                        : entryObject.get(QueryPath.VALUE).get(rightPath.getPath());
            }
            if (i == 0) {
                object.equal(leftValue);
            } else {
                builder.and(object.equal(leftValue));
            }
        }
        return builder;
    }
}
