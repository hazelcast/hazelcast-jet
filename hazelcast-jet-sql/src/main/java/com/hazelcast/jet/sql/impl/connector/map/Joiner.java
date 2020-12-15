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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.NestedLoopJoin;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.extract.QueryPath;

final class Joiner {

    private Joiner() {
    }

    static NestedLoopJoin join(
            DAG dag,
            String mapName,
            String tableName,
            JetJoinInfo joinInfo,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        int leftEquiJoinPrimitiveKeyIndex = leftEquiJoinPrimitiveKeyIndex(joinInfo, rightRowProjectorSupplier.paths());
        if (leftEquiJoinPrimitiveKeyIndex > -1) {
            return new NestedLoopJoin(
                    dag.newUniqueVertex(
                            "Join(Lookup-" + tableName + ")",
                            new JoinByPrimitiveKeyProcessorSupplier(
                                    joinInfo.isInner(),
                                    leftEquiJoinPrimitiveKeyIndex,
                                    joinInfo.condition(),
                                    mapName,
                                    rightRowProjectorSupplier
                            )
                    ),
                    edge -> edge.distributed().partitioned(extractPrimitiveKeyFn(leftEquiJoinPrimitiveKeyIndex))
            );
        } else if (joinInfo.isEquiJoin() && joinInfo.isInner()) {
            return new NestedLoopJoin(
                    dag.newUniqueVertex(
                            "Join(Predicate-" + tableName + ")",
                            JoinByPredicateInnerProcessorSupplier.supplier(joinInfo, mapName, rightRowProjectorSupplier)
                    ),
                    edge -> edge.distributed().fanout()
            );
        } else if (joinInfo.isEquiJoin() && joinInfo.isOuter()) {
            return new NestedLoopJoin(
                    dag.newUniqueVertex(
                            "Join(Predicate-" + tableName + ")",
                            new JoinByPredicateOuterProcessorSupplier(joinInfo, mapName, rightRowProjectorSupplier)
                    )
            );
        } else {
            return new NestedLoopJoin(
                    dag.newUniqueVertex(
                            "Join(Scan-" + tableName + ")",
                            new JoinScanProcessorSupplier(joinInfo, mapName, rightRowProjectorSupplier)
                    )
            );
        }
        // TODO: detect and handle always-false condition ?
    }

    private static int leftEquiJoinPrimitiveKeyIndex(JetJoinInfo joinInfo, QueryPath[] paths) {
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        for (int i = 0; i < rightEquiJoinIndices.length; i++) {
            QueryPath path = paths[rightEquiJoinIndices[i]];
            if (path.isTop() && path.isKey()) {
                return joinInfo.leftEquiJoinIndices()[i];
            }
        }
        return -1;
    }

    private static FunctionEx<Object, ?> extractPrimitiveKeyFn(int index) {
        return row -> {
            Object value = ((Object[]) row)[index];
            return value == null ? "" : value;
        };
    }
}
