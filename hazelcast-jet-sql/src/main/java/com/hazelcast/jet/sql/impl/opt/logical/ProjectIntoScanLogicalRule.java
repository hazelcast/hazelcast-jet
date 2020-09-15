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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

final class ProjectIntoScanLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new ProjectIntoScanLogicalRule();

    private ProjectIntoScanLogicalRule() {
        super(
                operand(Project.class, operand(TableScan.class, any())), RelFactories.LOGICAL_BUILDER,
                ProjectIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableScan scan = call.rel(1);

        Mappings.TargetMapping mapping = project.getMapping();

        if (mapping != null) {
            processSimple(call, mapping, scan);
        } else {
            processComplex(call, project, scan);
        }
    }

    private static void processSimple(RelOptRuleCall call, Mappings.TargetMapping mapping, TableScan scan) {
        HazelcastTable table = OptUtils.getHazelcastTable(scan);

        List<Integer> existingProjects = table.getProjects();
        List<Integer> convertedProjects = Mappings.apply((Mapping) mapping, existingProjects);

        RelOptTable convertedTable = OptUtils.createRelTable(
                (HazelcastRelOptTable) scan.getTable(),
                table.withProject(convertedProjects),
                scan.getCluster().getTypeFactory()
        );

        FullScanLogicalRel rel = new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                convertedTable,
                null
        );
        call.transformTo(rel);
    }

    private void processComplex(RelOptRuleCall call, Project project, TableScan scan) {
        HazelcastTable table = OptUtils.getHazelcastTable(scan);

        List<Integer> existingProjects = table.getProjects();
        ProjectFieldVisitor projectFieldVisitor = new ProjectFieldVisitor(existingProjects);
        for (RexNode node : project.getProjects()) {
            node.accept(projectFieldVisitor);
        }

        List<Integer> convertedProjects = projectFieldVisitor.createConvertedScanProjects();
        if (convertedProjects.size() == table.getProjects().size()) {
            // The Project operator already references all the fields from the TableScan. No trimming is possible, so
            // further optimization makes no sense.
            // E.g. "SELECT f3, f2, f1 FROM t" where t=[f1, f2, f3]
            return;
        }
        RelOptTable convertedTable = OptUtils.createRelTable(
                (HazelcastRelOptTable) scan.getTable(),
                table.withProject(convertedProjects),
                scan.getCluster().getTypeFactory()
        );

        ProjectConverter projectConverter = projectFieldVisitor.createProjectConverter();
        List<RexNode> convertedProjection = new ArrayList<>();
        for (RexNode node : project.getProjects()) {
            convertedProjection.add(node.accept(projectConverter));
        }

        FullScanLogicalRel rel = new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                convertedTable,
                convertedProjection
        );
        call.transformTo(rel);
    }

    private static final class ProjectFieldVisitor extends RexVisitorImpl<Void> {

        private final List<Integer> originalScanFields;

        private final Map<Integer, List<RexInputRef>> scanFieldIndexToProjectInputs = new LinkedHashMap<>();

        private ProjectFieldVisitor(List<Integer> originalScanFields) {
            super(true);

            this.originalScanFields = originalScanFields;
        }

        @Override
        public Void visitInputRef(RexInputRef projectInput) {
            // Get the scan field referenced by the given column expression (RexInputRef).
            // E.g., for the table t[t1, t2, t3] and the constrained TableScan[table=[t, project[2, 0]]], the input
            // reference [$0] (i.e. t3) is mapped to the scan field [2].
            Integer scanField = originalScanFields.get(projectInput.getIndex());

            assert scanField != null;

            // Track all column expressions from the Project operator that refer to the same TableScan field.
            List<RexInputRef> projectInputs =
                    scanFieldIndexToProjectInputs.computeIfAbsent(scanField, (k) -> new ArrayList<>(1));

            projectInputs.add(projectInput);

            return null;
        }

        @Override
        public Void visitCall(RexCall call) {
            for (RexNode operand : call.operands) {
                operand.accept(this);
            }

            return null;
        }

        private List<Integer> createConvertedScanProjects() {
            return new ArrayList<>(scanFieldIndexToProjectInputs.keySet());
        }

        private ProjectConverter createProjectConverter() {
            Map<RexNode, Integer> projectExpToScanFieldMap = new HashMap<>();

            for (Entry<Integer, List<RexInputRef>> projectInputs : scanFieldIndexToProjectInputs.entrySet()) {
                int index = projectInputs.getKey();
                for (RexNode projectInput : projectInputs.getValue()) {
                    projectExpToScanFieldMap.put(projectInput, index);
                }
            }

            return new ProjectConverter(projectExpToScanFieldMap);
        }
    }

    public static final class ProjectConverter extends RexShuttle {

        private final Map<RexNode, Integer> projectExpToScanFieldMap;

        private ProjectConverter(Map<RexNode, Integer> projectExpToScanFieldMap) {
            this.projectExpToScanFieldMap = projectExpToScanFieldMap;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            Integer index = projectExpToScanFieldMap.get(inputRef);

            assert index != null;

            return new RexInputRef(index, inputRef.getType());
        }
    }
}
