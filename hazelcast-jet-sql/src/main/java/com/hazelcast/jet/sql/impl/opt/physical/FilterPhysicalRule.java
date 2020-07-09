package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.FilterLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;

public final class FilterPhysicalRule extends RelOptRule {

    public static final RelOptRule INSTANCE = new FilterPhysicalRule();

    private FilterPhysicalRule() {
        super(
                OptUtils.parentChild(FilterLogicalRel.class, RelNode.class, LOGICAL),
                FilterPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FilterLogicalRel filter = call.rel(0);
        RelNode input = filter.getInput();

        RelNode convertedInput = OptUtils.toPhysicalInput(input);
        Collection<RelNode> transformedInputs = OptUtils.getPhysicalRelsFromSubset(convertedInput);
        for (RelNode transformedInput : transformedInputs) {
            FilterPhysicalRel newFilter = new FilterPhysicalRel(
                    filter.getCluster(),
                    transformedInput.getTraitSet(),
                    transformedInput,
                    filter.getCondition()
            );
            call.transformTo(newFilter);
        }
    }
}
