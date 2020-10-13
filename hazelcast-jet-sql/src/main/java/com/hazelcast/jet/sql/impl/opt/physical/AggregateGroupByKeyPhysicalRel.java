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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.ImmutableBitSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AggregateGroupByKeyPhysicalRel extends SingleRel implements PhysicalRel {

    private final ImmutableBitSet groupSet;
    private final AggregateOperation<?, Object[]> aggregateOperation;
    private final boolean distributed;

    AggregateGroupByKeyPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            AggregateOperation<?, Object[]> aggregateOperation,
            boolean distributed
    ) {
        super(cluster, traits, input);

        this.groupSet = groupSet;
        this.aggregateOperation = aggregateOperation;
        this.distributed = distributed;
    }

    public FunctionEx<Object[], Object> partitionKeyFn() {
        List<Integer> groupIndices = groupSet.toList();
        return row -> {
            Object[] key = new Object[groupIndices.size()];
            for (int i = 0; i < groupIndices.size(); i++) {
                key[i] = row[groupIndices.get(i)];
            }
            return new ObjectArray(key);
        };
    }

    public AggregateOperation<?, Object[]> aggregateOperation() {
        return aggregateOperation;
    }

    public boolean distributed() {
        return distributed;
    }

    @Override
    public PlanNodeSchema schema() {
        // intermediate operator, schema should not be ever needed
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex visit(CreateDagVisitor visitor) {
        return visitor.onGroupByKey(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                    .item("group", groupSet)
                    .item("distributed", distributed);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new AggregateGroupByKeyPhysicalRel(
                getCluster(),
                traitSet,
                sole(inputs),
                groupSet,
                aggregateOperation,
                distributed
        );
    }

    private static final class ObjectArray implements DataSerializable {

        private Object[] array;

        @SuppressWarnings("unused")
        private ObjectArray() {
        }

        private ObjectArray(Object[] array) {
            this.array = array;
        }

        @Override
        public String toString() {
            return "ObjectArray{" +
                    "array=" + Arrays.toString(array) +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ObjectArray that = (ObjectArray) o;
            return Arrays.equals(array, that.array);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(array);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            array = in.readObject();
        }
    }
}
