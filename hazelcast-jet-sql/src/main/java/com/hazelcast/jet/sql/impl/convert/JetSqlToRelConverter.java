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

package com.hazelcast.jet.sql.impl.convert;

import com.hazelcast.sql.impl.calcite.HazelcastSqlToRelConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;

public class JetSqlToRelConverter extends HazelcastSqlToRelConverter {

    public JetSqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,
            SqlValidator validator,
            CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config
    ) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    /*@Override
    protected void collectInsertTargets(
            SqlInsert call,
            final RexNode sourceRef,
            final List<String> targetColumnNames,
            List<RexNode> columnExprs) {

        HazelcastTable hazelcastTable = getTargetTable(call).unwrap(HazelcastTable.class);
        List<String> columnNames = new FilteringList(hazelcastTable);

        super.collectInsertTargets(call, sourceRef, columnNames, columnExprs);

        targetColumnNames.addAll(columnNames);
    }

    private static class FilteringList extends ArrayList<String> {

        private final HazelcastTable hazelcastTable;

        private FilteringList(HazelcastTable hazelcastTable) {
            this.hazelcastTable = hazelcastTable;
        }

        @Override
        public boolean add(String s) {
            return !hazelcastTable.isHidden(s) && super.add(s);
        }

        @Override
        public boolean addAll(Collection<? extends String> c) {
            boolean modified = false;
            for (String s : c) {
                modified |= add(s);
            }
            return modified;
        }
    }*/
}
