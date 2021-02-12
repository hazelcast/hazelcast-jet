package com.hazelcast.jet.sql.impl.opt.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

// TODO: rename ?
public class ReducedLogicalValues extends AbstractRelNode {

    private final RexNode filter;
    private final List<RexNode> project;
    private final ImmutableList<ImmutableList<RexLiteral>> tuples;

    public ReducedLogicalValues(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelDataType rowType,
            ImmutableList<ImmutableList<RexLiteral>> tuples,
            RexNode filter,
            List<RexNode> project
    ) {
        super(cluster, traitSet);
        this.rowType = rowType;

        this.filter = filter;
        this.project = project;
        this.tuples = tuples;
    }

    public static ReducedLogicalValues create(
            RelOptCluster cluster,
            RelDataType rowType,
            ImmutableList<ImmutableList<RexLiteral>> tuples,
            RexNode filter,
            List<RexNode> project
    ) {
        RelMetadataQuery mq = cluster.getMetadataQuery();
        RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
                .replaceIfs(RelCollationTraitDef.INSTANCE, () -> RelMdCollation.values(mq, rowType, tuples))
                .replaceIf(RelDistributionTraitDef.INSTANCE, () -> RelMdDistribution.values(rowType, tuples));
        return new ReducedLogicalValues(cluster, traitSet, rowType, tuples, filter, project);
    }

    public RexNode filter() {
        return filter;
    }

    public List<RexNode> project() {
        return project;
    }

    public ImmutableList<ImmutableList<RexLiteral>> tuples() {
        return tuples;
    }

    // TODO: computeSelfCost ?

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        assert inputs.isEmpty();

        return new ReducedLogicalValues(getCluster(), traitSet, getRowType(), tuples, filter, project);
    }
}

