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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.extract.QueryPath;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Traversers.singleton;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.util.Util.padRight;

@SuppressFBWarnings(
        value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "the class is never java-serialized"
)
final class JoinByPredicateOuterProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private JetJoinInfo joinInfo;
    private String mapName;
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    private transient IMap<Object, Object> map;
    private transient InternalSerializationService serializationService;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    private JoinByPredicateOuterProcessorSupplier() {
    }

    JoinByPredicateOuterProcessorSupplier(
            JetJoinInfo joinInfo,
            String mapName,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        assert joinInfo.isEquiJoin() && joinInfo.isOuter();

        this.joinInfo = joinInfo;
        this.mapName = mapName;
        this.rightRowProjectorSupplier = rightRowProjectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        map = context.jetInstance().getMap(mapName);
        serializationService = ((ProcSupplierCtx) context).serializationService();
        extractors = Extractors.newBuilder(serializationService).build();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            QueryPath[] rightPaths = rightRowProjectorSupplier.paths();
            KvRowProjector rightProjector = rightRowProjectorSupplier.get(serializationService, extractors);
            Processor processor =
                    new TransformP<Object[], Object[]>(joinFn(joinInfo, map, rightPaths, rightProjector)
                    ) {
                        @Override
                        public boolean isCooperative() {
                            return false;
                        }
                    };
            processors.add(processor);
        }
        return processors;
    }

    private static FunctionEx<Object[], Traverser<Object[]>> joinFn(
            JetJoinInfo joinInfo,
            IMap<Object, Object> map,
            QueryPath[] rightPaths,
            KvRowProjector rightRowProjector
    ) {
        int[] leftEquiJoinIndices = joinInfo.leftEquiJoinIndices();
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        BiFunctionEx<Object[], Object[], Object[]> joinFn = ExpressionUtil.joinFn(joinInfo.nonEquiCondition());

        return left -> {
            Predicate<Object, Object> predicate =
                    QueryUtil.toPredicate(left, leftEquiJoinIndices, rightEquiJoinIndices, rightPaths);
            if (predicate == null) {
                return singleton(padRight(left, rightRowProjector.getColumnCount()));
            }

            List<Object[]> joined = join(left, map.entrySet(predicate), rightRowProjector, joinFn);
            return joined.isEmpty()
                    ? singleton(padRight(left, rightRowProjector.getColumnCount()))
                    : traverseIterable(joined);
        };
    }

    private static List<Object[]> join(
            Object[] left,
            Set<Entry<Object, Object>> entries,
            KvRowProjector rightRowProjector,
            BiFunctionEx<Object[], Object[], Object[]> joinFn
    ) {
        List<Object[]> rows = new ArrayList<>();
        for (Entry<Object, Object> entry : entries) {
            Object[] right = rightRowProjector.project(entry);
            if (right == null) {
                continue;
            }

            Object[] joined = joinFn.apply(left, right);
            if (joined != null) {
                rows.add(joined);
            }
        }
        return rows;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(joinInfo);
        out.writeObject(mapName);
        out.writeObject(rightRowProjectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        joinInfo = in.readObject();
        mapName = in.readObject();
        rightRowProjectorSupplier = in.readObject();
    }
}
