/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.opt.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class LimitLogicalRel extends SingleRel implements LogicalRel {
    public final RexNode fetch;

    LimitLogicalRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode fetch) {
        super(cluster, traits, input);
        this.fetch = fetch;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LimitLogicalRel(getCluster(), traitSet, getInput(), fetch);
    }

    public RexNode getFetch() {
        return fetch;
    }
}
