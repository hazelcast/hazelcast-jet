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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

final class JoinProcessors {

    private JoinProcessors() {
    }

    static ProcessorSupplier processor(
            PartitionedMapTable table,
            Expression<Boolean> predicate,
            List<Expression<?>> projections,
            JetJoinInfo jetJoinInfo
    ) {
        String name = table.getMapName();
        List<TableField> fields = table.getFields();
        QueryPath[] paths = fields.stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
        QueryDataType[] types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);

        return new Supplier(
                name,
                paths,
                types,
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                predicate,
                projections,
                jetJoinInfo,
                resolveJoinProcessorFactory(jetJoinInfo, fields)
        );
    }

    private static JoinProcessorFactory resolveJoinProcessorFactory(JetJoinInfo jetJoinInfo, List<TableField> fields) {
        if (isEquiJoinByPrimitiveKey(jetJoinInfo, fields)) {
            return JoinByPrimitiveKeyProcessorFactory.INSTANCE;
        } else if (jetJoinInfo.isEquiJoin()) {
            return JoinByPredicateProcessorFactory.INSTANCE;
        } else {
            return JoinScanProcessorFactory.INSTANCE;
        }
    }

    private static boolean isEquiJoinByPrimitiveKey(JetJoinInfo jetJoinInfo, List<TableField> fields) {
        if (jetJoinInfo.rightEquiJoinIndices().length != 1) {
            return false;
        }

        MapTableField field = (MapTableField) fields.get(jetJoinInfo.rightEquiJoinIndices()[0]);
        QueryPath path = field.getPath();

        return path.isTop() && path.isKey();
    }

    @FunctionalInterface
    interface JoinProcessorFactory extends Serializable {

        Processor create(
                ServiceFactory<Object, IMap<Object, Object>> mapFactory,
                IMap<Object, Object> map,
                QueryPath[] rightPaths,
                SupplierEx<KvRowProjector> rightProjectorSupplier,
                JetJoinInfo jetJoinInfo
        );
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class Supplier implements ProcessorSupplier, DataSerializable {

        private String mapName;

        private QueryPath[] paths;
        private QueryDataType[] types;

        private QueryTargetDescriptor keyQueryDescriptor;
        private QueryTargetDescriptor valueQueryDescriptor;

        private Expression<Boolean> predicate;
        private List<Expression<?>> projection;

        private JetJoinInfo jetJoinInfo;

        private JoinProcessorFactory processorFactory;

        private transient ServiceFactory<Object, IMap<Object, Object>> mapFactory;
        private transient IMap<Object, Object> map;
        private transient InternalSerializationService serializationService;
        private transient Extractors extractors;

        @SuppressWarnings("unused")
        private Supplier() {
        }

        private Supplier(
                String mapName,
                QueryPath[] paths,
                QueryDataType[] types,
                QueryTargetDescriptor keyQueryDescriptor,
                QueryTargetDescriptor valueQueryDescriptor,
                Expression<Boolean> predicate,
                List<Expression<?>> projection,
                JetJoinInfo jetJoinInfo,
                JoinProcessorFactory processorFactory
        ) {
            this.mapName = mapName;

            this.paths = paths;
            this.types = types;

            this.keyQueryDescriptor = keyQueryDescriptor;
            this.valueQueryDescriptor = valueQueryDescriptor;

            this.predicate = predicate;
            this.projection = projection;

            this.jetJoinInfo = jetJoinInfo;

            this.processorFactory = processorFactory;
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
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Processor processor = processorFactory.create(
                        mapFactory,
                        map,
                        paths,
                        () -> new KvRowProjector(
                                paths,
                                types,
                                keyQueryDescriptor.create(serializationService, extractors, true),
                                valueQueryDescriptor.create(serializationService, extractors, false),
                                predicate,
                                projection
                        ),
                        jetJoinInfo
                );
                processors.add(processor);
            }
            return processors;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(mapName);
            out.writeInt(paths.length);
            for (QueryPath path : paths) {
                out.writeObject(path);
            }
            out.writeInt(types.length);
            for (QueryDataType type : types) {
                out.writeObject(type);
            }
            out.writeObject(keyQueryDescriptor);
            out.writeObject(valueQueryDescriptor);
            out.writeObject(predicate);
            out.writeObject(projection);
            out.writeObject(jetJoinInfo);
            out.writeObject(processorFactory);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readObject();
            paths = new QueryPath[in.readInt()];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = in.readObject();
            }
            types = new QueryDataType[in.readInt()];
            for (int i = 0; i < types.length; i++) {
                types[i] = in.readObject();
            }
            keyQueryDescriptor = in.readObject();
            valueQueryDescriptor = in.readObject();
            predicate = in.readObject();
            projection = in.readObject();
            jetJoinInfo = in.readObject();
            processorFactory = in.readObject();
        }
    }
}
