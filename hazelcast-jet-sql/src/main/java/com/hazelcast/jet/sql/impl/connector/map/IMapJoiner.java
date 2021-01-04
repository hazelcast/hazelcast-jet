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
import com.hazelcast.jet.sql.impl.connector.SqlConnector.VertexWithInputConfig;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.extract.QueryPath;

final class IMapJoiner {

    private IMapJoiner() {
    }

    static VertexWithInputConfig join(
            DAG dag,
            String mapName,
            String tableName,
            JetJoinInfo joinInfo,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        int leftEquiJoinPrimitiveKeyIndex = leftEquiJoinPrimitiveKeyIndex(joinInfo, rightRowProjectorSupplier.paths());
        if (leftEquiJoinPrimitiveKeyIndex > -1) {
            // This branch handles the case when there's an equi-join condition for the __key field.
            // For example: SELECT * FROM left [LEFT] JOIN right ON left.field1=right.__key /* ... more conditions on right */
            // In this case we'll use map.get() for the right map to get the matching entry by key and evaluate the
            // remaining conditions on the returned row.
            return new VertexWithInputConfig(
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
            // This branch handles the case when there's any equi-join, but not for __key (that was handled above)
            // For example: SELECT * FROM left JOIN right ON left.field1=right.field1
            // In this case we'll construct a com.hazelcast.query.Predicate that will find matching rows using
            // the `map.entrySet(predicate)` method.
            return new VertexWithInputConfig(
                    dag.newUniqueVertex("Join(Predicate-" + tableName + ")",
                            JoinByPredicateInnerProcessorSupplier.supplier(joinInfo, mapName, rightRowProjectorSupplier)),
                    edge -> edge.distributed().fanout());
        } else if (joinInfo.isEquiJoin() && joinInfo.isLeftOuter()) {
            return new VertexWithInputConfig(
                    dag.newUniqueVertex(
                            "Join(Predicate-" + tableName + ")",
                            new JoinByPredicateOuterProcessorSupplier(joinInfo, mapName, rightRowProjectorSupplier)
                    )
            );
        } else {
            return new VertexWithInputConfig(
                    dag.newUniqueVertex(
                            "Join(Scan-" + tableName + ")",
                            new JoinScanProcessorSupplier(joinInfo, mapName, rightRowProjectorSupplier)
                    )
            );
        }
        // TODO: detect and handle always-false condition ?
    }

    /**
     * Find the index of the field of the left side of a join that is in equals
     * predicate with the entire right-side key. Returns -1 if there's no
     * equi-join condition or the equi-join doesn't compare the key object of
     * the right side.
     * <p>
     * For example, in:
     * <pre>{@code
     *     SELECT *
     *     FROM l
     *     JOIN r ON l.field1=right.__key
     * }</pre>
     * it will return the index of {@code field1} in the left table.
     */
    private static int leftEquiJoinPrimitiveKeyIndex(JetJoinInfo joinInfo, QueryPath[] rightPaths) {
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        for (int i = 0; i < rightEquiJoinIndices.length; i++) {
            QueryPath path = rightPaths[rightEquiJoinIndices[i]];
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
