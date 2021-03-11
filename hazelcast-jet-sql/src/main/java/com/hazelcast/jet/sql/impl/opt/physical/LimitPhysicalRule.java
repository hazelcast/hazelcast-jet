package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.LimitLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.JetConventions.PHYSICAL;

final class LimitPhysicalRule  extends ConverterRule {
    static final RelOptRule INSTANCE = new LimitPhysicalRule();

    private LimitPhysicalRule() {
        super(
                LimitLogicalRel.class, LOGICAL, PHYSICAL,
                LimitPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        LimitLogicalRel logicalLimit = (LimitLogicalRel) rel;

        return new LimitPhysicalRel(
                logicalLimit.getCluster(),
                OptUtils.toPhysicalConvention(logicalLimit.getTraitSet()),
                OptUtils.toPhysicalInput(logicalLimit.getInput()),
                logicalLimit.getFetch(),
                logicalLimit.getRowType()
        );
    }
}
