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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converters;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.regex.Pattern;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class TypeCoercionTest extends SqlTestSupport {

    @Parameter
    public TestParams testParams;

    private final SqlService sqlService = instance().getSql();

    @Parameters(name="{0}")
    public static Object[] parameters() {
        return new Object[]{
                // NULL
//                TestParams.passingCase(1001, NULL, VARCHAR, "null", "null", null),
//                TestParams.passingCase(1002, NULL, BOOLEAN, "null", "null", null),
//                TestParams.passingCase(1003, NULL, TINYINT, "null", "null", null),
//                TestParams.passingCase(1004, NULL, SMALLINT, "null", "null", null),
//                TestParams.passingCase(1005, NULL, INTEGER, "null", "null", null),
//                TestParams.passingCase(1006, NULL, BIGINT, "null", "null", null),
//                TestParams.passingCase(1007, NULL, DECIMAL, "null", "null", null),
//                TestParams.passingCase(1008, NULL, REAL, "null", "null", null),
//                TestParams.passingCase(1009, NULL, DOUBLE, "null", "null", null),
//                TestParams.passingCase(1010, NULL, TIME, "null", "null", null),
//                TestParams.passingCase(1011, NULL, DATE, "null", "null", null),
//                TestParams.passingCase(1012, NULL, TIMESTAMP, "null", "null", null),
//                TestParams.passingCase(1013, NULL, TIMESTAMP_WITH_TIME_ZONE, "null", "null", null),
//                TestParams.failingCase(1014, NULL, OBJECT, "null",
//                        "null", "Writing to top-level fields of type OBJECT not supported"),
//
//                // VARCHAR
//                TestParams.passingCase(1101, VARCHAR, VARCHAR, "'foo'", "foo", "foo"),
//                TestParams.passingCase(1102, VARCHAR, BOOLEAN, "'true'", "true", true),
//                TestParams.passingCase(1103, VARCHAR, TINYINT, "'42'", "42", (byte) 42),
//                TestParams.failingCase(1104, VARCHAR, TINYINT, "'420'", "420",
//                        "Cannot parse VARCHAR value to TINYINT"),
//                TestParams.failingCase(1105, VARCHAR, TINYINT, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to TINYINT"),
//                TestParams.passingCase(1106, VARCHAR, SMALLINT, "'42'", "42", (short) 42),
//                TestParams.failingCase(1107, VARCHAR, SMALLINT, "'42000'", "42000",
//                        "Cannot parse VARCHAR value to SMALLINT"),
//                TestParams.failingCase(1108, VARCHAR, SMALLINT, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to SMALLINT"),
//                TestParams.passingCase(1109, VARCHAR, INTEGER, "'42'", "42", 42),
//                TestParams.failingCase(1110, VARCHAR, INTEGER, "'4200000000'", "4200000000",
//                        "Cannot parse VARCHAR value to INTEGER"),
//                TestParams.failingCase(1111, VARCHAR, INTEGER, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to INTEGER"),
//                TestParams.passingCase(1112, VARCHAR, BIGINT, "'42'", "42", 42L),
//                TestParams.failingCase(1113, VARCHAR, BIGINT, "'9223372036854775808000'", "9223372036854775808000",
//                        "Cannot parse VARCHAR value to BIGINT"),
//                TestParams.failingCase(1114, VARCHAR, BIGINT, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to BIGINT"),
//                TestParams.passingCase(1115, VARCHAR, DECIMAL, "'1.5'", "1.5", BigDecimal.valueOf(1.5)),
//                TestParams.failingCase(1116, VARCHAR, DECIMAL, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to DECIMAL"),
//                TestParams.passingCase(1117, VARCHAR, REAL, "'1.5'", "1.5", 1.5f),
//                TestParams.failingCase(1118, VARCHAR, REAL, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to REAL"),
//                TestParams.passingCase(1119, VARCHAR, DOUBLE, "'1.5'", "1.5", 1.5d),
//                TestParams.failingCase(1120, VARCHAR, DOUBLE, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to DOUBLE"),
//                TestParams.passingCase(1121, VARCHAR, TIME, "'01:42:00'", "01:42:00", LocalTime.of(1, 42)),
//                TestParams.failingCase(1122, VARCHAR, TIME, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to TIME"),
//                TestParams.passingCase(1123, VARCHAR, DATE, "'2020-12-30'", "2020-12-30", LocalDate.of(2020, 12, 30)),
//                TestParams.failingCase(1124, VARCHAR, DATE, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to DATE"),
//                TestParams.passingCase(1125, VARCHAR, TIMESTAMP, "'2020-12-30T01:42:00'", "2020-12-30T01:42:00", LocalDateTime.of(2020, 12, 30, 1, 42)),
//                TestParams.failingCase(1126, VARCHAR, TIMESTAMP, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to TIMESTAMP"),
//                TestParams.passingCase(1127, VARCHAR, TIMESTAMP_WITH_TIME_ZONE, "'2020-12-30T01:42:00-05:00'", "2020-12-30T01:42:00-05:00",
//                        OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5))),
//                TestParams.failingCase(1128, VARCHAR, TIMESTAMP_WITH_TIME_ZONE, "'foo'", "foo",
//                        "Cannot parse VARCHAR value to TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(1129, VARCHAR, OBJECT, "'foo'", "foo",
//                        "Writing to top-level fields of type OBJECT not supported"),
//
                // BOOLEAN
                TestParams.passingCase(1201, BOOLEAN, VARCHAR, "true", "true", "true"),
                TestParams.passingCase(1202, BOOLEAN, BOOLEAN, "true", "true", true),
                TestParams.failingCase(1203, BOOLEAN, TINYINT, "true", "true",
                        "CAST function cannot convert value of type BOOLEAN to type TINYINT"),
                TestParams.failingCase(1204, BOOLEAN, SMALLINT, "true", "true",
                        "CAST function cannot convert value of type BOOLEAN to type SMALLINT"),
                TestParams.failingCase(1205, BOOLEAN, INTEGER, "true", "true",
                        "CAST function cannot convert value of type BOOLEAN to type INTEGER"),
                TestParams.failingCase(1206, BOOLEAN, BIGINT, "true", "true",
                        "CAST function cannot convert value of type BOOLEAN to type BIGINT"),
                TestParams.failingCase(1207, BOOLEAN, DECIMAL, "true", "true",
                        "CAST function cannot convert value of type BOOLEAN to type DECIMAL"),
                TestParams.failingCase(1208, BOOLEAN, REAL, "true", "true",
                        "CAST function cannot convert value of type BOOLEAN to type REAL"),
                TestParams.failingCase(1209, BOOLEAN, DOUBLE, "true", "true",
                        "CAST function cannot convert value of type BOOLEAN to type DOUBLE"),
                TestParams.failingCase(1210, BOOLEAN, TIME, "true", "true",
                        "Cannot assign to target field 'this' of type TIME from source field '.+' of type BOOLEAN"),
                TestParams.failingCase(1211, BOOLEAN, DATE, "true", "true",
                        "Cannot assign to target field 'this' of type DATE from source field '.+' of type BOOLEAN"),
                TestParams.failingCase(1212, BOOLEAN, TIMESTAMP, "true", "true",
                        "Cannot assign to target field 'this' of type TIMESTAMP from source field '.+' of type BOOLEAN"),
                TestParams.failingCase(1213, BOOLEAN, TIMESTAMP_WITH_TIME_ZONE, "true", "true",
                        "Cannot assign to target field 'this' of type TIMESTAMP_WITH_TIME_ZONE from source field '.+' of type BOOLEAN"),
                TestParams.failingCase(1214, BOOLEAN, OBJECT, "true", "true",
                        "Writing to top-level fields of type OBJECT not supported"),

//                // TINYINT
//                TestParams.passingCase(1301, TINYINT, VARCHAR, "cast(132 as tinyint)", aaa, "42"),
//                TestParams.failingCase(1302, TINYINT, BOOLEAN, "cast(132 as tinyint)",
//                        valueTestSource, "cannot convert value of type TINYINT to type BOOLEAN"),
//                TestParams.passingCase(1303, TINYINT, TINYINT, "cast(132 as tinyint)", aaa, (byte) 42),
//                TestParams.passingCase(1304, TINYINT, SMALLINT, "cast(132 as tinyint)", aaa, (short) 42),
//                TestParams.passingCase(1305, TINYINT, INTEGER, "cast(132 as tinyint)", aaa, 42),
//                TestParams.passingCase(1306, TINYINT, BIGINT, "cast(132 as tinyint)", aaa, 42L),
//                TestParams.passingCase(1307, TINYINT, DECIMAL, "cast(132 as tinyint)", aaa, BigDecimal.valueOf(132)),
//                TestParams.passingCase(1308, TINYINT, REAL, "cast(132 as tinyint)", aaa, 42f),
//                TestParams.passingCase(1309, TINYINT, DOUBLE, "cast(132 as tinyint)", aaa, 42d),
//                TestParams.failingCase(1310, TINYINT, TIME, "cast(132 as tinyint)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TIME from source field 'EXPR$1' of type TINYINT"),
//                TestParams.failingCase(1311, TINYINT, DATE, "cast(132 as tinyint)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DATE from source field 'EXPR$1' of type TINYINT"),
//                TestParams.failingCase(1312, TINYINT, TIMESTAMP, "cast(132 as tinyint)",
//                        valueTestSource, "CAST function cannot convert value of type TINYINT to type TIMESTAMP"),
//                TestParams.failingCase(1313, TINYINT, TIMESTAMP_WITH_TIME_ZONE, "cast(132 as tinyint)",
//                        valueTestSource, "CAST function cannot convert value of type TINYINT to type TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(1314, TINYINT, OBJECT, "cast(132 as tinyint)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // SMALLINT
//                TestParams.passingCase(1401, SMALLINT, VARCHAR, "cast(42 as smallint)", aaa, "42"),
//                TestParams.failingCase(1402, SMALLINT, BOOLEAN, "cast(42 as smallint)",
//                        valueTestSource, "cannot convert value of type SMALLINT to type BOOLEAN"),
//                TestParams.passingCase(1403, SMALLINT, TINYINT, "cast(42 as smallint)", aaa, (byte) 42),
//                TestParams.failingCase(1404, SMALLINT, TINYINT, "cast(420 as smallint)",
//                        valueTestSource, "Numeric overflow while converting SMALLINT to TINYINT"),
//                TestParams.passingCase(1405, SMALLINT, SMALLINT, "cast(42 as smallint)", aaa, (short) 42),
//                TestParams.passingCase(1406, SMALLINT, INTEGER, "cast(42 as smallint)", aaa, 42),
//                TestParams.passingCase(1407, SMALLINT, BIGINT, "cast(42 as smallint)", aaa, 42L),
//                TestParams.passingCase(1408, SMALLINT, DECIMAL, "cast(42 as smallint)", aaa, BigDecimal.valueOf(42)),
//                TestParams.passingCase(1409, SMALLINT, REAL, "cast(42 as smallint)", aaa, 42f),
//                TestParams.passingCase(1410, SMALLINT, DOUBLE, "cast(42 as smallint)", aaa, 42d),
//                TestParams.failingCase(1411, SMALLINT, TIME, "cast(42 as smallint)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TIME from source field 'EXPR$1' of type SMALLINT"),
//                TestParams.failingCase(1412, SMALLINT, DATE, "cast(42 as smallint)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DATE from source field 'EXPR$1' of type SMALLINT"),
//                TestParams.failingCase(1413, SMALLINT, TIMESTAMP, "cast(42 as smallint)",
//                        valueTestSource, "CAST function cannot convert value of type SMALLINT to type TIMESTAMP"),
//                TestParams.failingCase(1414, SMALLINT, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as smallint)",
//                        valueTestSource, "CAST function cannot convert value of type SMALLINT to type TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(1415, SMALLINT, OBJECT, "cast(42 as smallint)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // INTEGER
//                TestParams.passingCase(1501, INTEGER, VARCHAR, "cast(42 as integer)", aaa, "42"),
//                TestParams.failingCase(1502, INTEGER, BOOLEAN, "cast(42 as integer)",
//                        valueTestSource, "cannot convert value of type INTEGER to type BOOLEAN"),
//                TestParams.passingCase(1503, INTEGER, TINYINT, "cast(42 as integer)", aaa, (byte) 42),
//                TestParams.failingCase(1504, INTEGER, TINYINT, "cast(420 as integer)",
//                        valueTestSource, "Numeric overflow while converting INTEGER to TINYINT"),
//                TestParams.passingCase(1505, INTEGER, SMALLINT, "cast(42 as integer)", aaa, (short) 42),
//                TestParams.failingCase(1506, INTEGER, SMALLINT, "cast(420000 as integer)",
//                        valueTestSource, "Numeric overflow while converting INTEGER to SMALLINT"),
//                TestParams.passingCase(1507, INTEGER, INTEGER, "cast(42 as integer)", aaa, 42),
//                TestParams.passingCase(1508, INTEGER, BIGINT, "cast(42 as integer)", aaa, 42L),
//                TestParams.passingCase(1509, INTEGER, DECIMAL, "cast(42 as integer)", aaa, BigDecimal.valueOf(42)),
//                TestParams.passingCase(1510, INTEGER, REAL, "cast(42 as integer)", aaa, 42f),
//                TestParams.passingCase(1511, INTEGER, DOUBLE, "cast(42 as integer)", aaa, 42d),
//                TestParams.failingCase(1512, INTEGER, TIME, "cast(42 as integer)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TIME from source field 'EXPR$1' of type INTEGER"),
//                TestParams.failingCase(1513, INTEGER, DATE, "cast(42 as integer)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DATE from source field 'EXPR$1' of type INTEGER"),
//                TestParams.failingCase(1514, INTEGER, TIMESTAMP, "cast(42 as integer)",
//                        valueTestSource, "CAST function cannot convert value of type INTEGER to type TIMESTAMP"),
//                TestParams.failingCase(1515, INTEGER, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as integer)",
//                        valueTestSource, "CAST function cannot convert value of type INTEGER to type TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(1516, INTEGER, OBJECT, "cast(42 as integer)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // BIGINT
//                TestParams.passingCase(1601, BIGINT, VARCHAR, "cast(42 as bigint)", aaa, "42"),
//                TestParams.failingCase(1602, BIGINT, BOOLEAN, "cast(42 as bigint)",
//                        valueTestSource, "cannot convert value of type BIGINT to type BOOLEAN"),
//                TestParams.passingCase(1603, BIGINT, TINYINT, "cast(42 as bigint)", aaa, (byte) 42),
//                TestParams.failingCase(1604, BIGINT, TINYINT, "cast(420 as bigint)",
//                        valueTestSource, "Numeric overflow while converting BIGINT to TINYINT"),
//                TestParams.passingCase(1605, BIGINT, SMALLINT, "cast(42 as bigint)", aaa, (short) 42),
//                TestParams.failingCase(1606, BIGINT, SMALLINT, "cast(420000 as bigint)",
//                        valueTestSource, "Numeric overflow while converting BIGINT to SMALLINT"),
//                TestParams.passingCase(1607, BIGINT, INTEGER, "cast(42 as bigint)", aaa, 42),
//                TestParams.failingCase(1608, BIGINT, INTEGER, "cast(4200000000 as bigint)",
//                        valueTestSource, "Numeric overflow while converting BIGINT to INTEGER"),
//                TestParams.passingCase(1609, BIGINT, BIGINT, "cast(42 as bigint)", aaa, 42L),
//                TestParams.passingCase(1610, BIGINT, DECIMAL, "cast(42 as bigint)", aaa, BigDecimal.valueOf(42)),
//                TestParams.passingCase(1611, BIGINT, REAL, "cast(42 as bigint)", aaa, 42f),
//                TestParams.passingCase(1612, BIGINT, DOUBLE, "cast(42 as bigint)", aaa, 42d),
//                TestParams.failingCase(1613, BIGINT, TIME, "cast(42 as bigint)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TIME from source field 'EXPR$1' of type BIGINT"),
//                TestParams.failingCase(1614, BIGINT, DATE, "cast(42 as bigint)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DATE from source field 'EXPR$1' of type BIGINT"),
//                TestParams.failingCase(1615, BIGINT, TIMESTAMP, "cast(42 as bigint)",
//                        valueTestSource, "CAST function cannot convert value of type BIGINT to type TIMESTAMP"),
//                TestParams.failingCase(1616, BIGINT, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as bigint)",
//                        valueTestSource, "CAST function cannot convert value of type BIGINT to type TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(1617, BIGINT, OBJECT, "cast(42 as bigint)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // DECIMAL
//                TestParams.passingCase(1701, DECIMAL, VARCHAR, "cast(42 as decimal)", aaa, "42"),
//                TestParams.failingCase(1702, DECIMAL, BOOLEAN, "cast(42 as decimal)",
//                        valueTestSource, "cannot convert value of type DECIMAL to type BOOLEAN"),
//                TestParams.passingCase(1703, DECIMAL, TINYINT, "cast(42 as decimal)", aaa, (byte) 42),
//                TestParams.failingCase(1704, DECIMAL, TINYINT, "cast(420 as decimal)",
//                        valueTestSource, "Numeric overflow while converting DECIMAL to TINYINT"),
//                TestParams.passingCase(1705, DECIMAL, SMALLINT, "cast(42 as decimal)", aaa, (short) 42),
//                TestParams.failingCase(1706, DECIMAL, SMALLINT, "cast(420000 as decimal)",
//                        valueTestSource, "Numeric overflow while converting DECIMAL to SMALLINT"),
//                TestParams.passingCase(1707, DECIMAL, INTEGER, "cast(42 as decimal)", aaa, 42),
//                TestParams.failingCase(1708, DECIMAL, INTEGER, "cast(4200000000 as decimal)",
//                        valueTestSource, "Numeric overflow while converting DECIMAL to INTEGER"),
//                TestParams.passingCase(1709, DECIMAL, BIGINT, "cast(42 as decimal)", aaa, 42L),
//                TestParams.failingCase(1710, DECIMAL, BIGINT, "cast(9223372036854775809 as decimal)",
//                        valueTestSource, "Numeric overflow while converting DECIMAL to BIGINT"),
//                TestParams.passingCase(1711, DECIMAL, DECIMAL, "cast(42 as decimal)", aaa, BigDecimal.valueOf(42)),
//                TestParams.passingCase(1712, DECIMAL, REAL, "cast(42 as decimal)", aaa, 42f),
//                TestParams.passingCase(1713, DECIMAL, DOUBLE, "cast(42 as decimal)", aaa, 42d),
//                TestParams.failingCase(1714, DECIMAL, TIME, "cast(42 as decimal)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TIME from source field 'EXPR$1' of type DECIMAL"),
//                TestParams.failingCase(1715, DECIMAL, DATE, "cast(42 as decimal)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DATE from source field 'EXPR$1' of type DECIMAL"),
//                TestParams.failingCase(1716, DECIMAL, TIMESTAMP, "cast(42 as decimal)",
//                        valueTestSource, "CAST function cannot convert value of type DECIMAL to type TIMESTAMP"),
//                TestParams.failingCase(1717, DECIMAL, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as decimal)",
//                        valueTestSource, "CAST function cannot convert value of type DECIMAL to type TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(1718, DECIMAL, OBJECT, "cast(42 as decimal)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // REAL
//                TestParams.passingCase(1801, REAL, VARCHAR, "cast(42 as real)", aaa, "42"),
//                TestParams.failingCase(1802, REAL, BOOLEAN, "cast(42 as real)",
//                        valueTestSource, "cannot convert value of type REAL to type BOOLEAN"),
//                TestParams.passingCase(1803, REAL, TINYINT, "cast(42 as real)", aaa, (byte) 42),
//                TestParams.failingCase(1804, REAL, TINYINT, "cast(420 as real)",
//                        valueTestSource, "Numeric overflow while converting REAL to TINYINT"),
//                TestParams.passingCase(1805, REAL, SMALLINT, "cast(42 as real)", aaa, (short) 42),
//                TestParams.failingCase(1806, REAL, SMALLINT, "cast(420000 as real)",
//                        valueTestSource, "Numeric overflow while converting REAL to SMALLINT"),
//                TestParams.passingCase(1807, REAL, INTEGER, "cast(42 as real)", aaa, 42),
//                TestParams.failingCase(1808, REAL, INTEGER, "cast(4200000000 as real)",
//                        valueTestSource, "Numeric overflow while converting REAL to INTEGER"),
//                TestParams.passingCase(1809, REAL, BIGINT, "cast(42 as real)", aaa, 42L),
//                TestParams.failingCase(1810, DECIMAL, BIGINT, "cast(18223372036854775808000 as real)",
//                        valueTestSource, "Numeric overflow while converting REAL to BIGINT"),
//                TestParams.passingCase(1811, REAL, DECIMAL, "cast(42 as real)", aaa, BigDecimal.valueOf(42)),
//                TestParams.passingCase(1812, REAL, REAL, "cast(42 as real)", aaa, 42f),
//                TestParams.passingCase(1813, REAL, DOUBLE, "cast(42 as real)", aaa, 42d),
//                TestParams.failingCase(1814, REAL, TIME, "cast(42 as real)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TIME from source field 'EXPR$1' of type REAL"),
//                TestParams.failingCase(1815, REAL, DATE, "cast(42 as real)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DATE from source field 'EXPR$1' of type REAL"),
//                TestParams.failingCase(1816, REAL, TIMESTAMP, "cast(42 as real)",
//                        valueTestSource, "CAST function cannot convert value of type REAL to type TIMESTAMP"),
//                TestParams.failingCase(1817, REAL, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as real)",
//                        valueTestSource, "CAST function cannot convert value of type REAL to type TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(1818, REAL, OBJECT, "cast(42 as real)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // DOUBLE
//                TestParams.passingCase(1901, DOUBLE, VARCHAR, "cast(42 as double)", aaa, "42"),
//                TestParams.failingCase(1902, DOUBLE, BOOLEAN, "cast(42 as double)",
//                        valueTestSource, "cannot convert value of type DOUBLE to type BOOLEAN"),
//                TestParams.passingCase(1903, DOUBLE, TINYINT, "cast(42 as double)", aaa, (byte) 42),
//                TestParams.failingCase(1904, DOUBLE, TINYINT, "cast(420 as double)",
//                        valueTestSource, "Numeric overflow while converting DOUBLE to TINYINT"),
//                TestParams.passingCase(1905, DOUBLE, SMALLINT, "cast(42 as double)", aaa, (short) 42),
//                TestParams.failingCase(1906, DOUBLE, SMALLINT, "cast(420000 as double)",
//                        valueTestSource, "Numeric overflow while converting DOUBLE to SMALLINT"),
//                TestParams.passingCase(1907, DOUBLE, INTEGER, "cast(42 as double)", aaa, 42),
//                TestParams.failingCase(1908, DOUBLE, INTEGER, "cast(4200000000 as double)",
//                        valueTestSource, "Numeric overflow while converting DOUBLE to INTEGER"),
//                TestParams.passingCase(1909, DOUBLE, BIGINT, "cast(42 as double)", aaa, 42L),
//                TestParams.failingCase(1910, DOUBLE, BIGINT, "cast(19223372036854775808000 as double)",
//                        valueTestSource, "Numeric overflow while converting DOUBLE to BIGINT"),
//                TestParams.passingCase(1911, DOUBLE, DECIMAL, "cast(42 as double)", aaa, BigDecimal.valueOf(42)),
//                TestParams.passingCase(1912, DOUBLE, REAL, "cast(42 as double)", aaa, 42f),
//                TestParams.passingCase(1912, DOUBLE, REAL, "cast(42e42 as double)", aaa, Float.POSITIVE_INFINITY),
//                TestParams.passingCase(1913, DOUBLE, DOUBLE, "cast(42 as double)", aaa, 42d),
//                TestParams.failingCase(1914, DOUBLE, TIME, "cast(42 as double)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TIME from source field 'EXPR$1' of type DOUBLE"),
//                TestParams.failingCase(1915, DOUBLE, DATE, "cast(42 as double)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DATE from source field 'EXPR$1' of type DOUBLE"),
//                TestParams.failingCase(1916, DOUBLE, TIMESTAMP, "cast(42 as double)",
//                        valueTestSource, "CAST function cannot convert value of type DOUBLE to type TIMESTAMP"),
//                TestParams.failingCase(1917, DOUBLE, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as double)",
//                        valueTestSource, "CAST function cannot convert value of type DOUBLE to type TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(1918, DOUBLE, OBJECT, "cast(42 as double)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // TIME
//                TestParams.passingCase(2001, TIME, VARCHAR, "cast('1:42:00' as time)", aaa, "1:42:00"),
//                TestParams.failingCase(2002, TIME, BOOLEAN, "cast('1:42:00' as time)",
//                        valueTestSource, "Cannot assign to target field 'this' of type BOOLEAN from source field 'EXPR$1' of type TIME"),
//                TestParams.failingCase(2003, TIME, TINYINT, "cast('1:42:00' as time)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TINYINT from source field 'EXPR$1' of type TIME"),
//                TestParams.failingCase(2004, TIME, SMALLINT, "cast('1:42:00' as time)",
//                        valueTestSource, "Cannot assign to target field 'this' of type SMALLINT from source field 'EXPR$1' of type TIME"),
//                TestParams.failingCase(2005, TIME, INTEGER, "cast('1:42:00' as time)",
//                        valueTestSource, "Cannot assign to target field 'this' of type INTEGER from source field 'EXPR$1' of type TIME"),
//                TestParams.failingCase(2006, TIME, BIGINT, "cast('1:42:00' as time)",
//                        valueTestSource, "Cannot assign to target field 'this' of type BIGINT from source field 'EXPR$1' of type TIME"),
//                TestParams.failingCase(2007, TIME, DECIMAL, "cast('1:42:00' as time)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DECIMAL from source field 'EXPR$1' of type TIME"),
//                TestParams.failingCase(2008, TIME, REAL, "cast('1:42:00' as time)",
//                        valueTestSource, "Cannot assign to target field 'this' of type REAL from source field 'EXPR$1' of type TIME"),
//                TestParams.failingCase(2009, TIME, DOUBLE, "cast('1:42:00' as time)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DOUBLE from source field 'EXPR$1' of type TIME"),
//                TestParams.passingCase(2010, TIME, TIME, "cast('1:42:00' as time)", aaa, LocalTime.of(1, 41)),
//                TestParams.failingCase(2011, TIME, DATE, "cast('1:42:00' as time)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DATE from source field 'EXPR$1' of type TIME"),
//                TestParams.failingCase(2012, TIME, TIMESTAMP, "cast('1:42:00' as time)",
//                        valueTestSource, "CAST function cannot convert value of type TIME to type TIMESTAMP"),
//                TestParams.failingCase(2013, TIME, TIMESTAMP_WITH_TIME_ZONE, "cast('1:42:00' as time)",
//                        valueTestSource, "CAST function cannot convert value of type TIME to type TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(2014, TIME, OBJECT, "cast('1:42:00' as time)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // DATE
//                TestParams.passingCase(2101, DATE, VARCHAR, "cast('2020-12-30' as date)", aaa, "2020-12-30"),
//                TestParams.failingCase(2102, DATE, BOOLEAN, "cast('2020-12-30' as date)",
//                        valueTestSource, "Cannot assign to target field 'this' of type BOOLEAN from source field 'EXPR$1' of type DATE"),
//                TestParams.failingCase(2103, DATE, TINYINT, "cast('2020-12-30' as date)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TINYINT from source field 'EXPR$1' of type DATE"),
//                TestParams.failingCase(2104, DATE, SMALLINT, "cast('2020-12-30' as date)",
//                        valueTestSource, "Cannot assign to target field 'this' of type SMALLINT from source field 'EXPR$1' of type DATE"),
//                TestParams.failingCase(2105, DATE, INTEGER, "cast('2020-12-30' as date)",
//                        valueTestSource, "Cannot assign to target field 'this' of type INTEGER from source field 'EXPR$1' of type DATE"),
//                TestParams.failingCase(2106, DATE, BIGINT, "cast('2020-12-30' as date)",
//                        valueTestSource, "Cannot assign to target field 'this' of type BIGINT from source field 'EXPR$1' of type DATE"),
//                TestParams.failingCase(2107, DATE, DECIMAL, "cast('2020-12-30' as date)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DECIMAL from source field 'EXPR$1' of type DATE"),
//                TestParams.failingCase(2108, DATE, REAL, "cast('2020-12-30' as date)",
//                        valueTestSource, "Cannot assign to target field 'this' of type REAL from source field 'EXPR$1' of type DATE"),
//                TestParams.failingCase(2109, DATE, DOUBLE, "cast('2020-12-30' as date)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DOUBLE from source field 'EXPR$1' of type DATE"),
//                TestParams.failingCase(2110, DATE, TIME, "cast('2020-12-30' as date)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TIME from source field 'EXPR$1' of type DATE"),
//                TestParams.passingCase(2111, DATE, DATE, "cast('2020-12-30' as date)", aaa, LocalDate.of(2020, 12, 30)),
//                TestParams.failingCase(2112, DATE, TIMESTAMP, "cast('2020-12-30' as date)",
//                        valueTestSource, "CAST function cannot convert value of type DATE to type DATESTAMP"),
//                TestParams.failingCase(2113, DATE, TIMESTAMP_WITH_TIME_ZONE, "cast('2020-12-30' as date)",
//                        valueTestSource, "CAST function cannot convert value of type DATE to type DATESTAMP_WITH_DATE_ZONE"),
//                TestParams.failingCase(2114, DATE, OBJECT, "cast('2020-12-30' as date)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // TIMESTAMP
//                TestParams.passingCase(2201, TIMESTAMP, VARCHAR, "cast('2020-12-30T01:42:00' as timestamp)", aaa, "2020-12-30T01:42:00"),
//                TestParams.failingCase(2202, TIMESTAMP, BOOLEAN, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Cannot assign to target field 'this' of type BOOLEAN from source field 'EXPR$1' of type TIMESTAMP"),
//                TestParams.failingCase(2203, TIMESTAMP, TINYINT, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TINYINT from source field 'EXPR$1' of type TIMESTAMP"),
//                TestParams.failingCase(2204, TIMESTAMP, SMALLINT, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Cannot assign to target field 'this' of type SMALLINT from source field 'EXPR$1' of type TIMESTAMP"),
//                TestParams.failingCase(2205, TIMESTAMP, INTEGER, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Cannot assign to target field 'this' of type INTEGER from source field 'EXPR$1' of type TIMESTAMP"),
//                TestParams.failingCase(2206, TIMESTAMP, BIGINT, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Cannot assign to target field 'this' of type BIGINT from source field 'EXPR$1' of type TIMESTAMP"),
//                TestParams.failingCase(2207, TIMESTAMP, DECIMAL, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DECIMAL from source field 'EXPR$1' of type TIMESTAMP"),
//                TestParams.failingCase(2208, TIMESTAMP, REAL, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Cannot assign to target field 'this' of type REAL from source field 'EXPR$1' of type TIMESTAMP"),
//                TestParams.failingCase(2209, TIMESTAMP, DOUBLE, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Cannot assign to target field 'this' of type DOUBLE from source field 'EXPR$1' of type TIMESTAMP"),
//                TestParams.failingCase(2210, TIMESTAMP, TIME, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Cannot assign to target field 'this' of type TIME from source field 'EXPR$1' of type TIMESTAMP"),
//                TestParams.passingCase(2211, TIMESTAMP, DATE, "cast('2020-12-30T01:42:00' as timestamp)", aaa, LocalDate.of(2020, 12, 30)),
//                TestParams.failingCase(2212, TIMESTAMP, TIMESTAMP, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "CAST function cannot convert value of type TIMESTAMP to type TIMESTAMP"),
//                TestParams.failingCase(2213, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "CAST function cannot convert value of type TIMESTAMP to type TIMESTAMP_WITH_TIMESTAMP_ZONE"),
//                TestParams.failingCase(2214, TIMESTAMP, OBJECT, "cast('2020-12-30T01:42:00' as timestamp)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // TIMESTAMP WITH TIME ZONE
//                TestParams.passingCase(2301, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)", aaa, "2020-12-30T01:42:00"),
//                TestParams.failingCase(2302, TIMESTAMP_WITH_TIME_ZONE, BOOLEAN, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        valueTestSource, "Cannot assign to target field 'this' of type BOOLEAN from source field 'EXPR$1' of type TIMESTAMP_WITH_TIME_ZONE"),
//                TestParams.failingCase(2303, TIMESTAMP_WITH_TIME_ZONE, TINYINT, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        valueTestSource, "CAST function cannot convert value of type TIMESTAMP_WITH_TIME_ZONE to type TINYINT"),
//                TestParams.failingCase(2304, TIMESTAMP_WITH_TIME_ZONE, SMALLINT, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        valueTestSource, "CAST function cannot convert value of type TIMESTAMP_WITH_TIME_ZONE to type SMALLINT"),
//                TestParams.failingCase(2305, TIMESTAMP_WITH_TIME_ZONE, INTEGER, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        valueTestSource, "CAST function cannot convert value of type TIMESTAMP_WITH_TIME_ZONE to type INTEGER"),
//                TestParams.failingCase(2306, TIMESTAMP_WITH_TIME_ZONE, BIGINT, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        valueTestSource, "CAST function cannot convert value of type TIMESTAMP_WITH_TIME_ZONE to type BIGINT"),
//                TestParams.failingCase(2307, TIMESTAMP_WITH_TIME_ZONE, DECIMAL, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        valueTestSource, "CAST function cannot convert value of type TIMESTAMP_WITH_TIME_ZONE to type DECIMAL"),
//                TestParams.failingCase(2308, TIMESTAMP_WITH_TIME_ZONE, REAL, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        valueTestSource, "CAST function cannot convert value of type TIMESTAMP_WITH_TIME_ZONE to type REAL"),
//                TestParams.failingCase(2309, TIMESTAMP_WITH_TIME_ZONE, DOUBLE, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        valueTestSource, "CAST function cannot convert value of type TIMESTAMP_WITH_TIME_ZONE to type DOUBLE"),
//                TestParams.passingCase(2310, TIMESTAMP_WITH_TIME_ZONE, TIME, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)", aaa, LocalTime.of(1, 42)),
//                TestParams.passingCase(2311, TIMESTAMP_WITH_TIME_ZONE, DATE, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)", aaa, LocalDate.of(2020, 12, 30)),
//                TestParams.passingCase(2312, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)", aaa, LocalDateTime.of(2020, 12, 30, 1, 42)),
//                TestParams.passingCase(2313, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        aaa, OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5))),
//                TestParams.failingCase(2314, TIMESTAMP_WITH_TIME_ZONE, OBJECT, "cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
//
//                // OBJECT
//                TestParams.passingCase(2401, OBJECT, VARCHAR, "cast('foo' as object)", aaa, "foo"),
//                TestParams.passingCase(2402, OBJECT, BOOLEAN, "cast(true as object)", aaa, true),
//                TestParams.passingCase(2403, OBJECT, TINYINT, "cast(42 as object)", aaa, (byte) 42),
//                TestParams.passingCase(2404, OBJECT, SMALLINT, "cast(420 as object)", aaa, (short) 420),
//                TestParams.passingCase(2405, OBJECT, INTEGER, "cast(420000 as object)", aaa, 420000),
//                TestParams.passingCase(2406, OBJECT, BIGINT, "cast(4200000000 as object)", aaa, 4200000000L),
//                TestParams.passingCase(2407, OBJECT, DECIMAL, "cast(cast(1.5 as decimal) as object)", aaa, BigDecimal.valueOf(1.5)),
//                TestParams.passingCase(2408, OBJECT, REAL, "cast(cast(1.5 as real) as object)", aaa, 1.5f),
//                TestParams.passingCase(2409, OBJECT, DOUBLE, "cast(cast(1.5 as double) as object)", aaa, 1.5d),
//                TestParams.passingCase(2410, OBJECT, TIME, "cast(cast('1:42:00' as time) as object)", aaa, LocalTime.of(1, 42)),
//                TestParams.passingCase(2411, OBJECT, DATE, "cast(cast('2020-12-30' as date) as object)", aaa, LocalDate.of(2020, 12, 30)),
//                TestParams.passingCase(2412, OBJECT, TIMESTAMP, "cast(cast('2020-12-30T01:42:00' as timestamp) as object)", aaa, LocalDateTime.of(2020, 12, 30, 1, 42)),
//                TestParams.passingCase(2413, OBJECT, TIMESTAMP_WITH_TIME_ZONE, "cast(cast('2020-12-30T01:42:00-05:00' as timestamp with local time zone) as object)",
//                        aaa, OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5))),
//                TestParams.failingCase(2414, OBJECT, OBJECT, "cast('2020-12-30T01:42:00-05:00' as object)",
//                        valueTestSource, "Writing to top-level fields of type OBJECT not supported"),
        };
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_insertValues() {
        Class<?> targetClass = javaClassForType(testParams.targetType);
        String sql = "CREATE MAPPING m type IMap " +
                "OPTIONS(" +
                "'keyFormat'='java', " +
                "'valueFormat'='java', " +
                "'keyJavaClass'='java.lang.Integer', " +
                "'valueJavaClass'='" + targetClass.getName() +
                "')";
        logger.info(sql);
        sqlService.execute(sql);
        sql = "SINK INTO m VALUES(0, " + testParams.valueLiteral + ")";
        logger.info(sql);
        try {
            sqlService.execute(sql);
            if (testParams.expectedFailureRegex != null) {
                fail("Expected to fail with \"" + testParams.expectedFailureRegex + "\", but no exception was thrown");
            }
        } catch (Exception e) {
            if (testParams.expectedFailureRegex == null) {
                throw e;
            }
            if (!testParams.exceptionMatches(e)) {
                throw new AssertionError("'" + e.getMessage() + "' didn't contain expected '"
                        + testParams.expectedFailureRegex + "'", e);
            } else {
                logger.info("Caught expected exception", e);
            }
        }
        Object actualValue = instance().getMap("m").get(0);
        assertEquals(testParams.targetValue, actualValue);
    }

    @Test
    public void test_insertSelect() {
        assumeTrue(testParams.srcType != NULL && testParams.targetType != NULL);
        Class<?> targetClass = javaClassForType(testParams.targetType);
        TestBatchSqlConnector.create(sqlService, "src", singletonList("v"),
                singletonList(resolveTypeForTypeFamily(testParams.srcType)),
                singletonList(new String[]{testParams.valueTestSource}));

        String sql = "CREATE MAPPING target TYPE IMap " +
                "OPTIONS(" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + targetClass.getName() +
                "')";
        logger.info(sql);
        sqlService.execute(sql);
        try {
            // TODO [viliam] remove the cast
            sql = "SINK INTO target SELECT cast(0 as integer), v FROM src";
            logger.info(sql);
            sqlService.execute(sql);
            if (testParams.expectedFailureRegex != null) {
                fail("Expected to fail with \"" + testParams.expectedFailureRegex + "\", but no exception was thrown");
            }
        } catch (Exception e) {
            if (testParams.expectedFailureRegex == null) {
                throw e;
            }
            if (!testParams.exceptionMatches(e)) {
                throw new AssertionError("'" + e.getMessage() + "' didn't contain expected '"
                        + testParams.expectedFailureRegex + "'", e);
            } else {
                logger.info("Caught expected exception", e);
            }
        }
        Object actualValue = instance().getMap("target").get(0);
        assertEquals(testParams.targetValue, actualValue);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private Class<?> javaClassForType(QueryDataTypeFamily type) {
        return Converters.getConverters().stream()
                         .filter(c -> c.getTypeFamily() == type)
                         .findAny()
                         .get()
                         .getNormalizedValueClass();
    }

    private static class TestParams {
        private final int testId;
        private final QueryDataTypeFamily srcType;
        private final QueryDataTypeFamily targetType;
        private final String valueLiteral;
        private final String valueTestSource;
        private final Object targetValue;
        private final Pattern expectedFailureRegex;

        private TestParams(int testId, QueryDataTypeFamily srcType, QueryDataTypeFamily targetType, String valueLiteral, String valueTestSource, Object targetValue, String expectedFailureRegex) {
            this.testId = testId;
            this.srcType = srcType;
            this.targetType = targetType;
            this.valueLiteral = valueLiteral;
            this.valueTestSource = valueTestSource;
            this.targetValue = targetValue;
            this.expectedFailureRegex = expectedFailureRegex != null ? Pattern.compile(expectedFailureRegex) : null;
        }

        static TestParams passingCase(int testId, QueryDataTypeFamily srcType, QueryDataTypeFamily targetType, String valueLiteral, String valueTestSource, Object targetValue) {
            return new TestParams(testId, srcType, targetType, valueLiteral, valueTestSource, targetValue, null);
        }

        static TestParams failingCase(int testId, QueryDataTypeFamily srcType, QueryDataTypeFamily targetType, String valueLiteral, String valueTestSource, String errorMsg) {
            return new TestParams(testId, srcType, targetType, valueLiteral, valueTestSource, null, errorMsg);
        }

        @Override
        public String toString() {
            return "TestParams{" +
                    "id=" + testId +
                    ", srcType=" + srcType +
                    ", targetType=" + targetType +
                    ", value=" + valueTestSource +
                    ", targetValue=" + targetValue +
                    ", expectedFailureRegex=" + expectedFailureRegex +
                    '}';
        }

        public boolean exceptionMatches(Exception e) {
            return expectedFailureRegex.matcher(e.getMessage()).find();
        }
    }
}