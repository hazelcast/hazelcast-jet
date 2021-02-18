/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converters;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.NULL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.REAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.resolveTypeForTypeFamily;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.fail;

@RunWith(JUnitParamsRunner.class)
public class SinkNullTypeCoercionTest extends SqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();

        kafkaTestSupport = new KafkaTestSupport();
        kafkaTestSupport.createKafkaCluster();
    }

    @AfterClass
    public static void tearDownClass() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @SuppressWarnings("unused")
    private Object[] parameters() {
        return new Object[]{
                TestParams.passingCase(1001, VARCHAR),
                TestParams.passingCase(1002, BOOLEAN),
                TestParams.passingCase(1003, TINYINT),
                TestParams.passingCase(1004, SMALLINT),
                TestParams.passingCase(1005, INTEGER),
                TestParams.passingCase(1006, BIGINT),
                TestParams.passingCase(1007, DECIMAL),
                TestParams.passingCase(1008, REAL),
                TestParams.passingCase(1009, DOUBLE),
                TestParams.passingCase(1010, TIME),
                TestParams.passingCase(1011, DATE),
                TestParams.passingCase(1012, TIMESTAMP),
                TestParams.passingCase(1013, TIMESTAMP_WITH_TIME_ZONE),
                TestParams.failingCase(1014, OBJECT, "Writing to top-level fields of type OBJECT not supported"),
        };
    }

    @Test
    @Parameters(method = "parameters")
    public void test_insertValues(TestParams parameters) {
        String name = createMapping(parameters.targetType);
        try {
            assertTopicEventually(name, "INSERT INTO " + name + " VALUES(0, null)", Tuple2.tuple2(0, null));

            if (parameters.expectedFailureRegex != null) {
                fail("Expected to fail with \"" + parameters.expectedFailureRegex + "\", but no exception was thrown");
            }
        } catch (Exception e) {
            if (parameters.expectedFailureRegex == null) {
                throw e;
            }
            if (!parameters.exceptionMatches(e)) {
                throw new AssertionError("\n'" + e.getMessage() + "'\ndidn't contain the regexp \n'"
                        + parameters.expectedFailureRegex + "'", e);
            } else {
                logger.info("Caught expected exception", e);
            }
        }
    }

    @Test
    @Parameters(method = "parameters")
    public void test_insertSelect(TestParams parameters) {
        TestBatchSqlConnector.create(sqlService, "src", singletonList("v"),
                singletonList(resolveTypeForTypeFamily(NULL)),
                singletonList(new String[]{null}));

        String name = createMapping(parameters.targetType);
        try {
            assertTopicEventually(name, "INSERT INTO " + name + " SELECT 0, v FROM src", Tuple2.tuple2(0, null));

            if (parameters.expectedFailureRegex != null) {
                fail("Expected to fail with \"" + parameters.expectedFailureRegex + "\", but no exception was thrown");
            }
        } catch (Exception e) {
            if (parameters.expectedFailureRegex == null /*&& parameters.expectedFailureNonLiteral == null*/) {
                throw new AssertionError("The query failed unexpectedly: " + e, e);
            }
            if (!parameters.exceptionMatches(e)) {
                throw new AssertionError("\n'" + e.getMessage() + "'\ndidn't contain the regexp \n'"
                        + parameters.expectedFailureRegex + "'", e);
            }
            logger.info("Caught expected exception", e);
        }
    }

    @Test
    @Parameters(method = "parameters")
    public void test_insertSelect_withLiteral(TestParams parameters) {
        TestBatchSqlConnector.create(sqlService, "src", 1);

        String name = createMapping(parameters.targetType);
        try {
            assertTopicEventually(name, "INSERT INTO " + name + " SELECT 0, null FROM src", Tuple2.tuple2(0, null));

            if (parameters.expectedFailureRegex != null) {
                fail("Expected to fail with \"" + parameters.expectedFailureRegex + "\", but no exception was thrown");
            }
        } catch (Exception e) {
            if (parameters.expectedFailureRegex == null) {
                throw e;
            }
            if (!parameters.exceptionMatches(e)) {
                throw new AssertionError("\n'" + e.getMessage() + "'\ndidn't contain the regexp \n'"
                        + parameters.expectedFailureRegex + "'", e);
            } else {
                logger.info("Caught expected exception", e);
            }
        }
    }

    private String createMapping(QueryDataTypeFamily type) {
        String name = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(name, INITIAL_PARTITION_COUNT);
        execute("CREATE MAPPING " + name + ' '
                + "TYPE Kafka "
                + "OPTIONS ("
                + "'keyFormat'='int'"
                + ", 'valueFormat'='java'"
                + ", 'valueJavaClass'='" + toJavaClassName(type) + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ", 'value.serializer'='" + FailingOnNonNullValueSerializer.class.getName() + '\''
                + ", 'value.deserializer'='" + FailingOnNonNullValueDeserializer.class.getName() + '\''
                + ")"
        );
        return name;
    }

    private void assertTopicEventually(String name, String sql, Tuple2<Integer, String> entry) {
        execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(name, createMap(entry.getKey(), entry.getValue()), false);
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(entry.getKey(), entry.getValue()))
        );
    }

    private void execute(String sql) {
        logger.info(sql);
        sqlService.execute(sql);
    }

    public static class FailingOnNonNullValueSerializer implements Serializer<Object> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, Object value) {
            Preconditions.checkTrue(value == null, String.valueOf(value));
            return null;
        }

        @Override
        public void close() {
        }
    }

    public static class FailingOnNonNullValueDeserializer implements Deserializer<Object> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public Object deserialize(String topic, byte[] bytes) {
            Preconditions.checkTrue(bytes == null, Arrays.toString(bytes));
            return null;
        }

        @Override
        public void close() {
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private String toJavaClassName(QueryDataTypeFamily type) {
        return Converters.getConverters().stream()
                .filter(converter -> converter.getTypeFamily() == type)
                .findAny()
                .get()
                .getNormalizedValueClass()
                .getName();
    }

    private static final class TestParams {

        private final int testId;
        private final QueryDataTypeFamily targetType;
        private final Pattern expectedFailureRegex;

        private TestParams(int testId, QueryDataTypeFamily targetType, String expectedFailureRegex) {
            this.testId = testId;
            this.targetType = targetType;
            this.expectedFailureRegex = expectedFailureRegex != null ? Pattern.compile(expectedFailureRegex) : null;
        }

        static TestParams passingCase(int testId, QueryDataTypeFamily targetType) {
            return new TestParams(testId, targetType, null);
        }

        @SuppressWarnings("SameParameterValue")
        static TestParams failingCase(int testId, QueryDataTypeFamily targetType, String errorMsg) {
            return new TestParams(testId, targetType, errorMsg);
        }

        @Override
        public String toString() {
            return "Parameters{" +
                    "id=" + testId +
                    ", targetType=" + targetType +
                    ", expectedFailureRegex=" + expectedFailureRegex +
                    '}';
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean exceptionMatches(Exception e) {
            return expectedFailureRegex.matcher(e.getMessage()).find();
        }
    }
}
