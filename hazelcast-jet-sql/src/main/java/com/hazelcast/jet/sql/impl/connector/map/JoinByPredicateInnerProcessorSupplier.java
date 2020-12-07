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

import com.hazelcast.cluster.Address;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.extract.QueryPath;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.empty;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

@SuppressFBWarnings(
        value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "the class is never java-serialized"
)
final class JoinByPredicateInnerProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private JetJoinInfo joinInfo;
    private String mapName;
    private int partitionCount;
    private List<Integer> partitions;
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    private transient MapProxyImpl<Object, Object> map;
    private transient InternalSerializationService serializationService;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    private JoinByPredicateInnerProcessorSupplier() {
    }

    JoinByPredicateInnerProcessorSupplier(
            JetJoinInfo joinInfo,
            String mapName,
            int partitionCount,
            List<Integer> partitions,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        assert joinInfo.isEquiJoin() && joinInfo.isInner();

        this.joinInfo = joinInfo;
        this.mapName = mapName;
        this.partitionCount = partitionCount;
        this.partitions = partitions;
        this.rightRowProjectorSupplier = rightRowProjectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        map = (MapProxyImpl<Object, Object>) context.jetInstance().getMap(mapName);
        serializationService = ((ProcSupplierCtx) context).serializationService();
        extractors = Extractors.newBuilder(serializationService).build();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            PartitionIdSet partitions = new PartitionIdSet(partitionCount, this.partitions);
            QueryPath[] rightPaths = rightRowProjectorSupplier.paths();
            KvRowProjector rightProjector = rightRowProjectorSupplier.get(serializationService, extractors);
            Processor processor =
                    new TransformP<Object[], Object[]>(joinFn(joinInfo, map, partitions, rightPaths, rightProjector)
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
            MapProxyImpl<Object, Object> map,
            PartitionIdSet partitions,
            QueryPath[] rightPaths,
            KvRowProjector rightRowProjector
    ) {
        int[] leftEquiJoinIndices = joinInfo.leftEquiJoinIndices();
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        BiFunctionEx<Object[], Object[], Object[]> joinFn = ExpressionUtil.joinFn(joinInfo.nonEquiCondition());

        return left -> {
            Predicate<Object, Object> predicate =
                    JoinPredicateFactory.toPredicate(left, leftEquiJoinIndices, rightEquiJoinIndices, rightPaths);
            if (predicate == null) {
                return empty();
            }

            List<Object[]> joined = join(left, map.entrySet(predicate, partitions.copy()), rightRowProjector, joinFn);
            return traverseIterable(joined);
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
        out.writeInt(partitionCount);
        out.writeObject(partitions);
        out.writeObject(rightRowProjectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        joinInfo = in.readObject();
        mapName = in.readObject();
        partitionCount = in.readInt();
        partitions = in.readObject();
        rightRowProjectorSupplier = in.readObject();
    }

    static ProcessorMetaSupplier supplier(
            JetJoinInfo joinInfo,
            String mapName,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        return new Supplier(joinInfo, mapName, rightRowProjectorSupplier);
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class Supplier implements ProcessorMetaSupplier, DataSerializable {

        private JetJoinInfo joinInfo;
        private String mapName;
        private KvRowProjector.Supplier rightRowProjectorSupplier;

        private transient PartitionService partitionService;

        @SuppressWarnings("unused")
        private Supplier() {
        }

        private Supplier(
                JetJoinInfo joinInfo,
                String mapName,
                KvRowProjector.Supplier rightRowProjectorSupplier
        ) {
            assert joinInfo.isEquiJoin() && joinInfo.isInner();

            this.joinInfo = joinInfo;
            this.mapName = mapName;
            this.rightRowProjectorSupplier = rightRowProjectorSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            this.partitionService = context.jetInstance().getHazelcastInstance().getPartitionService();
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            Set<Partition> partitions = partitionService.getPartitions();
            int partitionCount = partitions.size();
            Map<Address, List<Integer>> partitionsByMember = Util.assignPartitions(
                    addresses,
                    partitions.stream()
                              .collect(groupingBy(
                                      partition -> partition.getOwner().getAddress(),
                                      mapping(Partition::getPartitionId, toList()))
                              )
            );

            return address -> new JoinByPredicateInnerProcessorSupplier(
                    joinInfo,
                    mapName,
                    partitionCount,
                    partitionsByMember.get(address),
                    rightRowProjectorSupplier
            );
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
}
