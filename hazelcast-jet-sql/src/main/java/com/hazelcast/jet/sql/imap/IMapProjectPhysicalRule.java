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

package com.hazelcast.jet.sql.imap;

import com.hazelcast.jet.sql.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This rule converts logical projection into physical projection. Physical projection inherits distribution of the
 * underlying operator.
 */
public final class IMapProjectPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new IMapProjectPhysicalRule();

    private IMapProjectPhysicalRule() {
        super(
            OptUtils.parentChild(IMapProjectLogicalRel.class, RelNode.class, OptUtils.CONVENTION_LOGICAL),
            IMapProjectPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IMapProjectLogicalRel logicalProject = call.rel(0);
        RelNode input = logicalProject.getInput();

        RelNode convertedInput = OptUtils.toPhysicalInput(input);

        Collection<InputAndTraitSet> transforms = getTransforms(logicalProject, convertedInput);
        System.out.println("aaa onMatch0 in " + getClass().getSimpleName() + ", transforms=" + transforms);

        for (InputAndTraitSet transform : transforms) {
            IMapProjectPhysicalRel newProject = new IMapProjectPhysicalRel(
                logicalProject.getCluster(),
                transform.getTraitSet(),
                transform.getInput(),
                logicalProject.getProjects(),
                logicalProject.getRowType()
            );

            call.transformTo(newProject);
        }
    }

    /**
     * Get conversions which will be applied to the given logical project.
     *
     * @param logicalProject Logical project.
     * @param convertedInput Input.
     * @return Conversions (converted input + trait set).
     */
    private static Collection<InputAndTraitSet> getTransforms(IMapProjectLogicalRel logicalProject, RelNode convertedInput) {
        // TODO: Handle collation properly.
        List<InputAndTraitSet> res = new ArrayList<>(1);

        // Get mapping of project input fields to an index of related expression in the projection.
        Map<Integer, Integer> candCollationFields = getCandidateCollationFields(logicalProject);

        Collection<RelNode> physicalInputs = OptUtils.getPhysicalRelsFromSubset(convertedInput);

        for (RelNode physicalInput : physicalInputs) {
            RelTraitSet finalTraitSet = createPhysicalTraitSet(physicalInput, candCollationFields);

            if (finalTraitSet != null) {
                res.add(new InputAndTraitSet(physicalInput, finalTraitSet));
            }
        }

//        if (res.isEmpty()) {
//            // If there were no physical inputs, then just propagate the default distribution.
//            RelTraitSet finalTraitSet = createPhysicalTraitSet(convertedInput, candDistFields, candCollationFields);
//
//            res.add(new InputAndTraitSet(convertedInput, finalTraitSet));
//        }

        return res;
    }

    /**
     * Create a trait set for physical project.
     *
     * @param physicalInput Project's input.
     * @param candCollationFields Candidate collation fields.
     * @return Trait set.
     */
    private static RelTraitSet createPhysicalTraitSet(
        RelNode physicalInput,
        Map<Integer, Integer> candCollationFields
    ) {
        RelCollation finalCollation = deriveCollation(physicalInput, candCollationFields);

        return OptUtils.traitPlus(physicalInput.getTraitSet(), finalCollation);
    }

    /**
     * Derive collation of physical projection from the logical projection and physical input. The condition is that
     * logical projection should have at least some prefix of input fields.
     *
     * @param physicalInput Physical input.
     * @param candCollationFields Candidate collation fields.
     * @return Collation which should be used for physical projection.
     */
    private static RelCollation deriveCollation(RelNode physicalInput, Map<Integer, Integer> candCollationFields) {
        RelCollation inputCollation = physicalInput.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

        List<RelFieldCollation> fields = null;

        for (RelFieldCollation field : inputCollation.getFieldCollations()) {
            Integer projectFieldIndex = candCollationFields.get(field.getFieldIndex());

            if (projectFieldIndex != null) {
                if (fields == null) {
                    fields = new ArrayList<>(1);
                }

                fields.add(new RelFieldCollation(projectFieldIndex, field.getDirection(), field.nullDirection));
            } else {
                // No more prefixes. We are done.
                break;
            }
        }

        if (fields == null) {
            fields = Collections.emptyList();
        }

        return RelCollations.of(fields);
    }

    /**
     * Get a map of candidate collation fields for the given logical project. Key of the map is the index of the field
     * in its input, value of the map is the index of that field in projection.
     *
     * @param project Project.
     * @return Map of candidate collation fields.
     */
    private static Map<Integer, Integer> getCandidateCollationFields(IMapProjectLogicalRel project) {
        Map<Integer, Integer> res = new HashMap<>();

        int idx = 0;

        for (RexNode node : project.getProjects()) {
            // Only direct field references are capable of maintaining collation.
            // Nested fields are not supported, since they loss the collation. E.g. a stream of data sorted on "a"
            // is not guaranteed to be sorted on "a.b".
            // CAST function is not applicable here as wel, because collation rules may be different for different
            // data types, and concrete data type is only resolved in runtime. For example, integer input of values
            // 1, 2, 11 is sorted in that order. But if the field is converted to VARCHAR, then sort order is
            // different: "1", "11", "2".

            // TODO: Some functions may preserve monotonicity, e.g. "x + 1" is always sorted in the same way as "x"
            // TODO: provided that precision loss is prohibited. Investigate Calcite's SqlMonotonicity.

            // TODO:
            if (node instanceof RexInputRef) {
                RexInputRef node0 = (RexInputRef) node;

                res.put(node0.getIndex(), idx);
            }

            idx++;
        }

        return res;
    }

    /**
     * A pair of input and trait set which should be used for transformation.
     */
    private static final class InputAndTraitSet {
        /** Input of the projection. */
        private final RelNode input;

        /** Trait set of the projection. */
        private final RelTraitSet traitSet;

        private InputAndTraitSet(RelNode input, RelTraitSet traitSet) {
            this.input = input;
            this.traitSet = traitSet;
        }

        public RelNode getInput() {
            return input;
        }

        public RelTraitSet getTraitSet() {
            return traitSet;
        }
    }
}
