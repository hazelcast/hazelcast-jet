package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.sql.impl.JetPlan.CreateExternalMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropExternalMappingPlan;
import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@RunWith(JUnitParamsRunner.class)
public class JetPlanExecutorTest {

    @InjectMocks
    private JetPlanExecutor planExecutor;

    @Mock
    private MappingCatalog catalog;

    @Mock
    private JetInstance jetInstance;

    @Mock
    private Map<QueryId, QueryResultProducer> resultConsumerRegistry;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    @Parameters({
            "true, false",
            "false, true"
    })
    public void test_createExternalMappingExecution(boolean replace, boolean ifNotExists) {
        // given
        Mapping mapping = mapping();
        CreateExternalMappingPlan plan = new CreateExternalMappingPlan(mapping, replace, ifNotExists, planExecutor);

        // when
        SqlResult result = planExecutor.execute(plan);

        // then
        assertThat(result.isUpdateCount()).isTrue();
        assertThat(result.updateCount()).isEqualTo(-1);
        verify(catalog).createMapping(mapping, replace, ifNotExists);
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_dropExternalMappingExecution(boolean ifExists) {
        // given
        String name = "name";
        DropExternalMappingPlan plan = new DropExternalMappingPlan(name, ifExists, planExecutor);

        // when
        SqlResult result = planExecutor.execute(plan);

        // then
        assertThat(result.isUpdateCount()).isTrue();
        assertThat(result.updateCount()).isEqualTo(-1);
        verify(catalog).removeMapping(name, ifExists);
    }

    private static Mapping mapping() {
        return new Mapping("name", "type", emptyList(), emptyMap());
    }
}