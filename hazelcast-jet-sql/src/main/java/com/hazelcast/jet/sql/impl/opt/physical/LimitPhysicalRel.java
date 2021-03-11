package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

public class LimitPhysicalRel extends AbstractRelNode implements PhysicalRel {
    private final RelNode input;
    private final RexNode fetch;
    private final RelDataType rowType;

    public LimitPhysicalRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode fetch, RelDataType rowType) {
        super(cluster, traitSet);
        this.input = input;
        this.fetch = fetch;
        this.rowType = rowType;
    }

    public Expression<?> fetch() {
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema());
        return fetch.accept(visitor);
    }

    public RelNode getInput() {
        return input;
    }

    @Override
    public PlanNodeSchema schema() {
        return OptUtils.schema(rowType);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onLimit(this);
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }
}
