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

package com.hazelcast.jet.sql;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.sql.cost.CostFactory;
import com.hazelcast.jet.sql.imap.IMapProjectPhysicalRule;
import com.hazelcast.jet.sql.imap.IMapScanPhysicalRule;
import com.hazelcast.jet.sql.schema.JetSchema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.JetRootCalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class JetSqlService {

    /** Converter: whether to convert LogicalTableScan to some physical form immediately or not. We do not need this. */
    private static final boolean CONVERTER_CONVERT_TABLE_ACCESS = false;

    /** Converter: whether to trim unused fields. */
    private static final boolean CONVERTER_TRIM_UNUSED_FIELDS = true;

    /**
     * Converter: whether to expand subqueries. When set to {@code false}, subqueries are left as is in the form of
     * {@link org.apache.calcite.rex.RexSubQuery}. Otherwise they are expanded into {@link org.apache.calcite.rel.core.Correlate}
     * instances.
     * Do not enable this because you may run into https://issues.apache.org/jira/browse/CALCITE-3484. Instead, subquery
     * elimination rules are executed during logical planning. In addition, resulting plans are slightly better that those
     * produced by "expand" flag.
     */
    private static final boolean CONVERTER_EXPAND = false;

    /** Singleton instance. */
    public static final RelMetadataProvider METADATA_PROVIDER_INSTANCE = ChainedRelMetadataProvider.of(ImmutableList
            .of(RowCountMetadata.SOURCE, DefaultRelMetadataProvider.INSTANCE));

    private final JetInstance instance;
    private final JetSchema schema;
    private final SqlValidator validator;
    private final SqlToRelConverter sqlToRelConverter;
    private final VolcanoPlanner planner;

    public JetSqlService(JetInstance instance) {
        this.instance = instance;
        this.schema = new JetSchema(instance);
        this.validator = createValidator();

        JavaTypeFactory typeFactory = new HazelcastTypeFactory();
        CalciteConnectionConfig connectionConfig = createConnectionConfig();
        Prepare.CatalogReader catalogReader = createCatalogReader(typeFactory, connectionConfig, schema);
        planner = createPlanner(connectionConfig);
        RelOptCluster cluster = createCluster(planner, typeFactory);
        this.sqlToRelConverter = createSqlToRelConverter(catalogReader, validator, cluster);
    }

    private static RelOptCluster createCluster(VolcanoPlanner planner, JavaTypeFactory typeFactory) {
        // TODO: Use CachingRelMetadataProvider instead?
        RelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(METADATA_PROVIDER_INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        cluster.setMetadataProvider(relMetadataProvider);

        return cluster;
    }

    private static VolcanoPlanner createPlanner(CalciteConnectionConfig config) {
        VolcanoPlanner planner = new VolcanoPlanner(
                CostFactory.INSTANCE,
                Contexts.of(config)
        );

        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
//        planner.addRelTraitDef(DistributionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        return planner;
    }

    private static SqlToRelConverter createSqlToRelConverter(
            Prepare.CatalogReader catalogReader,
            SqlValidator validator,
            RelOptCluster cluster
    ) {
        SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder = SqlToRelConverter.configBuilder()
                .withConvertTableAccess(CONVERTER_CONVERT_TABLE_ACCESS)
                .withTrimUnusedFields(CONVERTER_TRIM_UNUSED_FIELDS)
                .withExpand(CONVERTER_EXPAND);

        return new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                sqlToRelConfigBuilder.build());
    }

    /**
     * Parse SQL statement.
     *
     * @param sqlQuery SQL string.
     * @return SQL tree.
     */
    private SqlNode parse(String sqlQuery) {
        SqlNode node;

        try {
            SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();

            parserConfig.setCaseSensitive(true);
            parserConfig.setUnquotedCasing(Casing.UNCHANGED);
            parserConfig.setQuotedCasing(Casing.UNCHANGED);
            parserConfig.setConformance(HazelcastSqlConformance.INSTANCE);

            SqlParser parser = SqlParser.create(sqlQuery, parserConfig.build());

            node = parser.parseStmt();
        } catch (Exception e) {
            throw new JetException("Failed to parse SQL: " + e, e);
        }

        return validator.validate(node);
    }

    /**
     * Perform initial conversion of an SQL tree to a relational tree.
     *
     * @param node SQL tree.
     * @return Relational tree.
     */
    private RelNode convert(SqlNode node) {
        // 1. Perform initial conversion.
        RelRoot root = sqlToRelConverter.convertQuery(node, false, true);

        // 2. Remove subquery expressions, converting them to Correlate nodes.
        RelNode relNoSubqueries = rewriteSubqueries(root.rel);

        // 3. Perform decorrelation, i.e. rewrite a nested loop where the right side depends on the value of the left side,
        // to a variation of joins, semijoins and aggregations, which could be executed much more efficiently.
        // See "Unnesting Arbitrary Queries", Thomas Neumann and Alfons Kemper.
        RelNode relDecorrelated = sqlToRelConverter.decorrelate(node, relNoSubqueries);

        // 4. The side effect of subquery rewrite and decorrelation in Apache Calcite is a number of unnecessary fields,
        // primarily in projections. This steps removes unused fields from the tree.
        RelNode relTrimmed = sqlToRelConverter.trimUnusedFields(true, relDecorrelated);

        return relTrimmed;
    }

    /**
     * Executes a DML query. Currently only {@code INSERT INTO ... SELECT ...}
     * is supported.
     */
    public Job execute(String sql) {
        SqlNode node = parse(sql);

        RelNode rel = convert(node);

        System.out.println("before logical opt:\n" + RelOptUtil.toString(rel));
        LogicalRel logicalRel = optimizeLogical(rel);
        System.out.println("after logical opt:\n" + RelOptUtil.toString(logicalRel));
        PhysicalRel physicalRel = optimizePhysical(logicalRel);
        System.out.println("after physical opt:\n" + RelOptUtil.toString(physicalRel));

        DAG dag = createDag(physicalRel, null);
        // submit the job
        return instance.newJob(dag);
    }

    /**
     * Executes a DQL query (a SELECT query).
     */
    public Observable<Object[]> executeQuery(String sql) {
        SqlNode node = parse(sql);

        RelNode rel = convert(node);

        LogicalRel logicalRel = optimizeLogical(rel);
        System.out.println(RelOptUtil.toString(logicalRel));
        PhysicalRel physicalRel = optimizePhysical(logicalRel);
        System.out.println(RelOptUtil.toString(physicalRel));

        String observableName = "sql-sink-" + UUID.randomUUID().toString();
        DAG dag = createDag(physicalRel, SinkProcessors.writeObservableP(observableName));
        // submit the job
        instance.newJob(dag);
        return instance.getObservable(observableName);
    }

    private DAG createDag(PhysicalRel physicalRel, ProcessorMetaSupplier sinkSupplier) {
        DAG dag = new DAG();
        Vertex sink = sinkSupplier != null ? dag.newVertex("sink", sinkSupplier) : null;

        CreateDagVisitor visitor = new CreateDagVisitor(dag, sink);
        physicalRel.visit(visitor);
        return dag;
    }

    /**
     * Perform logical optimization.
     *
     * @param rel Original logical tree.
     * @return Optimized logical tree.
     */
    private LogicalRel optimizeLogical(RelNode rel) {
        RuleSet rules = JetLogicalRules.getRuleSet();
        Program program = Programs.of(rules);

        RelNode res = program.run(
                planner,
                rel,
                OptUtils.toLogicalConvention(rel.getTraitSet()),
                ImmutableList.of(),
                ImmutableList.of()
        );

        return (LogicalRel) res;
    }

    /**
     * Perform physical optimization. This is where proper access methods and algorithms for joins and aggregations are chosen.
     *
     * @param rel Optimized logical tree.
     * @return Optimized physical tree.
     */
    private PhysicalRel optimizePhysical(RelNode rel) {
        RuleSet rules = RuleSets.ofList(
                JetTableInsertPhysicalRule.INSTANCE,
//                SortPhysicalRule.INSTANCE,
//                RootPhysicalRule.INSTANCE,
//                FilterPhysicalRule.INSTANCE,
                IMapProjectPhysicalRule.INSTANCE,
                IMapScanPhysicalRule.INSTANCE,
//                AggregatePhysicalRule.INSTANCE,
//                JoinPhysicalRule.INSTANCE,

                new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER)
        );

        Program program = Programs.of(rules);
        RelNode res = program.run(
                planner,
                rel,
                OptUtils.toPhysicalConvention(rel.getTraitSet()),
                ImmutableList.of(),
                ImmutableList.of());

        return (PhysicalRel) res;
    }

    public JetSchema getSchema() {
        return schema;
    }

    /**
     * Special substep of an initial query conversion which eliminates correlated subqueries, converting them to various forms
     * of joins. It is used instead of "expand" flag due to bugs in Calcite (see {@link #CONVERTER_EXPAND}).
     *
     * @param rel Initial relation.
     * @return Resulting relation.
     */
    private RelNode rewriteSubqueries(RelNode rel) {
        HepProgramBuilder hepPgmBldr = new HepProgramBuilder();

        hepPgmBldr.addRuleInstance(SubQueryRemoveRule.FILTER);
        hepPgmBldr.addRuleInstance(SubQueryRemoveRule.PROJECT);
        hepPgmBldr.addRuleInstance(SubQueryRemoveRule.JOIN);

        HepPlanner planner = new HepPlanner(hepPgmBldr.build(), Contexts.empty(), true, null, RelOptCostImpl.FACTORY);

        planner.setRoot(rel);

        return planner.findBestExp();
    }

    private SqlValidator createValidator() {
        SqlOperatorTable opTab = SqlStdOperatorTable.instance();

        HazelcastTypeFactory typeFactory = new HazelcastTypeFactory();
        CalciteConnectionConfig connectionConfig = createConnectionConfig();
        Prepare.CatalogReader catalogReader = createCatalogReader(typeFactory, connectionConfig, schema);

        return new HazelcastSqlValidator(
                opTab,
                catalogReader,
                typeFactory,
                HazelcastSqlConformance.INSTANCE
        );
    }

    private static CalciteConnectionConfig createConnectionConfig() {
        Properties properties = new Properties();

        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        properties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        return new CalciteConnectionConfigImpl(properties);
    }

    private static Prepare.CatalogReader createCatalogReader(
            JavaTypeFactory typeFactory,
            CalciteConnectionConfig config,
            JetSchema rootSchema
    ) {
        return new CalciteCatalogReader(
                new JetRootCalciteSchema(rootSchema),
                Collections.emptyList(),
                typeFactory,
                config
        );
    }
}
