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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.processor.AsyncTransformUsingServiceOrderedP;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.entry;

@SuppressFBWarnings(
        value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "the class is never java-serialized"
)
final class JoinByPrimitiveKeyProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private static final int MAX_CONCURRENT_OPS = 8;

    private JetJoinInfo joinInfo;
    private String mapName;
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    private transient ServiceFactory<Object, IMap<Object, Object>> mapFactory;
    private transient IMap<Object, Object> map;
    private transient InternalSerializationService serializationService;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    private JoinByPrimitiveKeyProcessorSupplier() {
    }

    JoinByPrimitiveKeyProcessorSupplier(
            JetJoinInfo joinInfo,
            String mapName,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        assert joinInfo.isEquiJoin() && joinInfo.leftEquiJoinIndices().length == 1;

        this.joinInfo = joinInfo;
        this.mapName = mapName;
        this.rightRowProjectorSupplier = rightRowProjectorSupplier;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(@Nonnull Context context) {
        mapFactory = (ServiceFactory<Object, IMap<Object, Object>>) ServiceFactories.iMapService(mapName);
        map = context.jetInstance().getMap(mapName);
        serializationService = ((ProcSupplierCtx) context).serializationService();
        extractors = Extractors.newBuilder(serializationService).build();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        SupplierEx<KvRowProjector> rightRowProjectorSupplier =
                () -> this.rightRowProjectorSupplier.get(serializationService, extractors);

        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Processor processor = new AsyncTransformUsingServiceOrderedP<>(
                    mapFactory,
                    map,
                    MAX_CONCURRENT_OPS,
                    joinFn(joinInfo, rightRowProjectorSupplier)
            );
            processors.add(processor);
        }
        return processors;
    }

    private static BiFunctionEx<IMap<Object, Object>, Object[], CompletableFuture<Traverser<Object[]>>> joinFn(
            JetJoinInfo joinInfo,
            SupplierEx<KvRowProjector> rightRowProjectorSupplier
    ) {
        int leftEquiJoinIndex = joinInfo.leftEquiJoinIndices()[0];
        BiFunctionEx<Object[], Object[], Object[]> joinFn = ExpressionUtil.joinFn(joinInfo.nonEquiCondition());

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

                          Object[] right = rightRowProjectorSupplier.get().project(entry(key, value));
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(mapName);
        out.writeObject(rightRowProjectorSupplier);
        out.writeObject(joinInfo);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readObject();
        rightRowProjectorSupplier = in.readObject();
        joinInfo = in.readObject();
    }
}
