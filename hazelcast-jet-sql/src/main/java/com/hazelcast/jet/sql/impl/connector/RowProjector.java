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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.jet.sql.impl.ExpressionUtil.evaluate;

public class RowProjector implements Row {

    private final QueryTarget target;
    private final QueryExtractor[] extractors;

    private final Expression<Boolean> predicate;
    private final List<Expression<?>> projection;

    @SuppressWarnings("unchecked")
    RowProjector(
            String[] paths,
            QueryDataType[] types,
            QueryTarget target,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        this.target = target;
        this.extractors = createExtractors(target, paths, types);

        this.predicate = predicate != null ? predicate
                : (Expression<Boolean>) ConstantExpression.create(true, QueryDataType.BOOLEAN);
        this.projection = projection;
    }

    private static QueryExtractor[] createExtractors(
            QueryTarget target,
            String[] paths,
            QueryDataType[] types
    ) {
        QueryExtractor[] extractors = new QueryExtractor[paths.length];
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            QueryDataType type = types[i];

            extractors[i] = target.createExtractor(path, type);
        }
        return extractors;
    }

    public Object[] project(Object object) {
        target.setTarget(object);

        if (!Boolean.TRUE.equals(evaluate(predicate, this))) {
            return null;
        }

        Object[] row = new Object[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            row[i] = evaluate(projection.get(i), this);
        }
        return row;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int index) {
        return (T) extractors[index].get();
    }

    @Override
    public int getColumnCount() {
        return extractors.length;
    }

    public static Supplier supplier(
            String[] paths,
            QueryDataType[] types,
            SupplierEx<QueryTarget> targetSupplier,
            Expression<Boolean> predicate,
            List<Expression<?>> projections
    ) {
        return new Supplier(paths, types, targetSupplier, predicate, projections);
    }

    public static class Supplier implements DataSerializable {

        private String[] paths;
        private QueryDataType[] types;

        private SupplierEx<QueryTarget> targetSupplier;

        private Expression<Boolean> predicate;
        private List<Expression<?>> projections;

        @SuppressWarnings("unused")
        private Supplier() {
        }

        Supplier(
                String[] paths,
                QueryDataType[] types,
                SupplierEx<QueryTarget> targetSupplier,
                Expression<Boolean> predicate,
                List<Expression<?>> projections
        ) {
            this.paths = paths;
            this.types = types;
            this.targetSupplier = targetSupplier;
            this.predicate = predicate;
            this.projections = projections;
        }

        public RowProjector get() {
            return new RowProjector(paths, types, targetSupplier.get(), predicate, projections);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(paths.length);
            for (String path : paths) {
                out.writeUTF(path);
            }
            out.writeInt(types.length);
            for (QueryDataType type : types) {
                out.writeObject(type);
            }
            out.writeObject(targetSupplier);
            out.writeObject(predicate);
            out.writeObject(projections);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            paths = new String[in.readInt()];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = in.readUTF();
            }
            types = new QueryDataType[in.readInt()];
            for (int i = 0; i < types.length; i++) {
                types[i] = in.readObject();
            }
            targetSupplier = in.readObject();
            predicate = in.readObject();
            projections = in.readObject();
        }
    }
}
