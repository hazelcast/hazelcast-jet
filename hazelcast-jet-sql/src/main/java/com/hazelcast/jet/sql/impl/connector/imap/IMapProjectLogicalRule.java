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

package com.hazelcast.jet.sql.impl.connector.imap;

import com.hazelcast.jet.sql.impl.OptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;

import static com.hazelcast.jet.sql.impl.OptUtils.CONVENTION_LOGICAL;

public final class IMapProjectLogicalRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new IMapProjectLogicalRule();

    private IMapProjectLogicalRule() {
        super(
                LogicalProject.class,
                Convention.NONE,
                CONVENTION_LOGICAL,
                IMapProjectLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        Project project = (Project) rel;
        RelNode input = project.getInput();

        return new IMapProjectLogicalRel(
                project.getCluster(),
                OptUtils.toLogicalConvention(project.getTraitSet()),
                OptUtils.toLogicalInput(input),
                project.getProjects(),
                project.getRowType()
        );
    }
}
