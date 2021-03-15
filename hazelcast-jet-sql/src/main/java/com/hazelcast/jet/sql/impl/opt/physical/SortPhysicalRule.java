package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.SortLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.JetConventions.PHYSICAL;

final class SortPhysicalRule extends ConverterRule {
    static final RelOptRule INSTANCE = new SortPhysicalRule();

    private SortPhysicalRule() {
        super(
                SortLogicalRel.class, LOGICAL, PHYSICAL,
                SortPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        SortLogicalRel logicalLimit = (SortLogicalRel) rel;

        return new SortPhysicalRel(
                logicalLimit.getCluster(),
                OptUtils.toPhysicalConvention(logicalLimit.getTraitSet()),
                OptUtils.toPhysicalInput(logicalLimit.getInput()),
                logicalLimit.getCollation(),
                logicalLimit.offset,
                logicalLimit.getFetch(),
                logicalLimit.getRowType()
        );
    }
}
