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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDigestIncludeType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class ValuesLogicalRel extends AbstractRelNode implements LogicalRel {

    private final RelDataType rowType;
    private final List<RexNode> filters;
    private final List<List<RexNode>> projects;
    private final List<ImmutableList<ImmutableList<RexLiteral>>> tuples;

    ValuesLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelDataType rowType,
            List<ImmutableList<ImmutableList<RexLiteral>>> tuples
    ) {
        this(cluster, traits, rowType, singletonList(null), singletonList(null), tuples);
    }

    ValuesLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelDataType rowType,
            List<RexNode> filters,
            List<List<RexNode>> projects,
            List<ImmutableList<ImmutableList<RexLiteral>>> tuples
    ) {
        super(cluster, traits);

        this.rowType = rowType;
        this.filters = filters;
        this.projects = projects;
        this.tuples = tuples;
    }

    public List<RexNode> filters() {
        return filters;
    }

    public List<List<RexNode>> projects() {
        return projects;
    }

    public List<ImmutableList<ImmutableList<RexLiteral>>> tuples() {
        return tuples;
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ValuesLogicalRel(getCluster(), traitSet, rowType, filters, projects, tuples);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // A little adapter just to get the tuples to come out
        // with curly brackets instead of square brackets.  Plus
        // more whitespace for readability.
        RelWriter writer = super.explainTerms(pw)
                // For rel digest, include the row type since a rendered
                // literal may leave the type ambiguous (e.g. "null").
                .itemIf("type", rowType, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
                .itemIf("type", rowType.getFieldList(), pw.nest());
        if (pw.nest()) {
            pw.item("tuples", tuples);
        } else {
            pw.item("tuples",
                    tuples.stream()
                            .map(row -> row.stream()
                                    .flatMap(Collection::stream)
                                    .map(literal -> literal.computeDigest(RexDigestIncludeType.NO_TYPE))
                                    .collect(Collectors.joining(", ", "{ ", " }")))
                            .collect(Collectors.joining(", ", "[", "]")));
        }
        pw.item("filters", filters);
        pw.item("projects", projects);
        return writer;
    }
}
