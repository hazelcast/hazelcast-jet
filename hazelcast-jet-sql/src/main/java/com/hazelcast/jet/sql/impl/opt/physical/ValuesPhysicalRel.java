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

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.joining;

public class ValuesPhysicalRel extends AbstractRelNode implements PhysicalRel {

    private final RelDataType rowType;
    private final List<Object[]> values;

    ValuesPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelDataType rowType,
            List<Object[]> values
    ) {
        super(cluster, traits);

        this.rowType = rowType;
        this.values = Collections.unmodifiableList(values);
    }

    public List<Object[]> values() {
        PlanNodeSchema schema = schema();
        List<Object[]> values = new ArrayList<>(this.values.size());
        for (Object[] item : this.values) {
            Object[] row = new Object[item.length];
            for (int i = 0; i < row.length; i++) {
                row[i] = schema.getType(i).convert(item[i]);
            }
            values.add(row);
        }
        return values;
    }

    @Override
    public PlanNodeSchema schema() {
        List<QueryDataType> fieldTypes = OptUtils.extractFieldTypes(getRowType());
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public Vertex visit(CreateDagVisitor visitor) {
        return visitor.onValues(this);
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                    .item("values", values.stream().map(Arrays::toString).collect(joining(", ")));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ValuesPhysicalRel(getCluster(), traitSet, rowType, values);
    }
}
