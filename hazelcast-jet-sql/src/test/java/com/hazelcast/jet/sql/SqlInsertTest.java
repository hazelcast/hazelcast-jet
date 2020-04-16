package com.hazelcast.jet.sql;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.schema.JetSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_VALUE_CLASS;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class SqlInsertTest extends SimpleTestInClusterSupport {

    private static final String PERSON_MAP_SINK = "person_map_sink";
    private static final String ALL_TYPES_MAP = "all_types_map";

    private static JetSqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = new JetSqlService(instance());

        sqlService.createTable(PERSON_MAP_SINK, JetSchema.IMAP_LOCAL_SERVER,
                ImmutableMap.of(TO_KEY_CLASS, Person.class.getName(), TO_VALUE_CLASS, Person.class.getName()),
                singletonList(
                        entry("birthday", QueryDataType.DATE)
                )
        );

        // an IMap with a field of every type
        sqlService.createTable(ALL_TYPES_MAP, JetSchema.IMAP_LOCAL_SERVER,
                createMap(TO_VALUE_CLASS, AllTypesValue.class.getName()),
                asList(
                        entry("__key", QueryDataType.DECIMAL_BIG_INTEGER),
                        entry("string", QueryDataType.VARCHAR),
                        entry("character0", QueryDataType.VARCHAR_CHARACTER),
                        entry("character1", QueryDataType.VARCHAR_CHARACTER),
                        entry("boolean0", QueryDataType.BOOLEAN),
                        entry("boolean1", QueryDataType.BOOLEAN),
                        entry("byte0", QueryDataType.TINYINT),
                        entry("byte1", QueryDataType.TINYINT),
                        entry("short0", QueryDataType.SMALLINT),
                        entry("short1", QueryDataType.SMALLINT),
                        entry("int0", QueryDataType.INT),
                        entry("int1", QueryDataType.INT),
                        entry("long0", QueryDataType.BIGINT),
                        entry("long1", QueryDataType.BIGINT),
                        entry("bigDecimal", QueryDataType.DECIMAL),
                        entry("bigInteger", QueryDataType.DECIMAL_BIG_INTEGER),
                        entry("float0", QueryDataType.REAL),
                        entry("float1", QueryDataType.REAL),
                        entry("double0", QueryDataType.DOUBLE),
                        entry("double1", QueryDataType.DOUBLE),
                        entry("localTime", QueryDataType.TIME),
                        entry("localDate", QueryDataType.DATE),
                        entry("localDateTime", QueryDataType.TIMESTAMP),
                        entry("date", QueryDataType.TIMESTAMP_WITH_TZ_DATE),
                        entry("calendar", QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR),
                        entry("instant", QueryDataType.TIMESTAMP_WITH_TZ_INSTANT),
                        entry("zonedDateTime", QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME),
                        entry("offsetDateTime", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME)/*,
                        entry("yearMonthInterval", QueryDataType.INTERVAL_YEAR_MONTH),
                        entry("daySecondInterval", QueryDataType.INTERVAL_DAY_SECOND)*/));
    }

    @Test
    public void insert_null() {
        assertMap(
                PERSON_MAP_SINK, "INSERT INTO " + PERSON_MAP_SINK + "(birthday) VALUES (null)",
                createMap(new Person(), new Person()));
    }

    @Test
    public void insert_value_shadows_key() {
        assertMap(
                PERSON_MAP_SINK, "INSERT INTO " + PERSON_MAP_SINK + "(birthday) VALUES ('2020-01-01')",
                createMap(new Person(), new Person(LocalDate.of(2020, 1, 1))));
    }

    @Test
    public void insert_allTypes() {
        assertMap(ALL_TYPES_MAP, "INSERT INTO " + ALL_TYPES_MAP + " VALUES (" +
                        "1, --key\n" +
                        "'string', --varchar\n" +
                        "'a', --character\n" +
                        "'b',\n" +
                        "true, --boolean\n" +
                        "false,\n" +
                        "126, --byte\n" +
                        "127, \n" +
                        "32766, --short\n" +
                        "32767, \n" +
                        "2147483646, --int \n" +
                        "2147483647,\n" +
                        "9223372036854775806, --long\n" +
                        "9223372036854775807,\n" +
                        // this is bigDecimal, but it's still limited to 64-bit unscaled value, see
                        // SqlValidatorImpl.validateLiteral()
                        "9223372036854775.123, --bigDecimal\n" +
                        "9223372036854775222, --bigInteger\n" +
                        "1234567890.1, --float\n" +
                        "1234567890.2, \n" +
                        "123451234567890.1, --double\n" +
                        "123451234567890.2,\n" +
                        "time'12:23:34', -- local time\n" +
                        "date'2020-04-15', -- local date \n" +
                        "timestamp'2020-04-15 12:23:34.1', --timestamp\n" +
                        // there's no timestamp-with-tz literal in calcite apparently
                        "timestamp'2020-04-15 12:23:34.2', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.3', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.4', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.5', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.6' --timestamp with tz\n" +
                        /*"INTERVAL '1' YEAR, -- year-to-month interval\n" +
                        "INTERVAL '1' HOUR -- day-to-second interval\n" +*/
                        ")",
                createMap(BigInteger.valueOf(1), new AllTypesValue(
                        "string",
                        'a',
                        'b',
                        true,
                        false,
                        (byte) 126,
                        (byte) 127,
                        (short) 32766,
                        (short) 32767,
                        2147483646,
                        2147483647,
                        9223372036854775806L,
                        9223372036854775807L,
                        new BigDecimal("9223372036854775.123"),
                        new BigInteger("9223372036854775222"),
                        1234567890.1f,
                        1234567890.2f,
                        123451234567890.1,
                        123451234567890.2,
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 100_000_000),
                        Date.from(Instant.ofEpochMilli(1586953414200L)),
                        (GregorianCalendar) new Calendar.Builder()
                                .setTimeZone(TimeZone.getTimeZone(UTC))
                                .setLocale(Locale.getDefault(Locale.Category.FORMAT))
                                .setInstant(1586953414300L)
                                .build(),
                        Instant.ofEpochMilli(1586953414400L),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 500_000_000, UTC),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 600_000_000, UTC)
                )));
    }

    @Test
    public void insert_allTypes_as_strings() {
        assertMap(ALL_TYPES_MAP, "INSERT INTO " + ALL_TYPES_MAP + " VALUES (" +
                        "'1', --key\n" +
                        "'string', --varchar\n" +
                        "'a', --character\n" +
                        "'b',\n" +
                        "'true', --boolean\n" +
                        "'false',\n" +
                        "'126', --byte\n" +
                        "'127', \n" +
                        "'32766', --short\n" +
                        "'32767', \n" +
                        "'2147483646', --int \n" +
                        "'2147483647',\n" +
                        "'9223372036854775806', --long\n" +
                        "'9223372036854775807',\n" +
                        "'9223372036854775.123', --bigDecimal\n" +
                        "'9223372036854775222', --bigInteger\n" +
                        "'1234567890.1', --float\n" +
                        "'1234567890.2', \n" +
                        "'123451234567890.1', --double\n" +
                        "'123451234567890.2',\n" +
                        "'12:23:34', -- local time\n" +
                        "'2020-04-15', -- local date \n" +
                        "'2020-04-15T12:23:34.1', --timestamp\n" +
                        "'2020-04-15T12:23:34.2Z', --timestamp with tz\n" +
                        "'2020-04-15T12:23:34.3Z', --timestamp with tz\n" +
                        "'2020-04-15T12:23:34.4Z', --timestamp with tz\n" +
                        "'2020-04-15T12:23:34.5Z', --timestamp with tz\n" +
                        "'2020-04-15T12:23:34.6Z' --timestamp with tz\n" +
                        /*"INTERVAL '1' YEAR, -- year-to-month interval\n" +
                        "INTERVAL '1' HOUR -- day-to-second interval\n" +*/
                        ")",
                createMap(BigInteger.valueOf(1), new AllTypesValue(
                        "string",
                        'a',
                        'b',
                        true,
                        false,
                        (byte) 126,
                        (byte) 127,
                        (short) 32766,
                        (short) 32767,
                        2147483646,
                        2147483647,
                        9223372036854775806L,
                        9223372036854775807L,
                        new BigDecimal("9223372036854775.123"),
                        new BigInteger("9223372036854775222"),
                        1234567890.1f,
                        1234567890.2f,
                        123451234567890.1,
                        123451234567890.2,
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 100_000_000),
                        Date.from(Instant.ofEpochMilli(1586953414200L)),
                        GregorianCalendar.from(ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 300_000_000, UTC)),
                        Instant.ofEpochMilli(1586953414400L),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 500_000_000, UTC),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 600_000_000, UTC)
                )));
    }

    private <K, V> void assertMap(String name, String sql, Map<K, V> expected) {
        sqlService.execute(sql).join();
        assertEquals(expected, new HashMap<>(instance().getMap(name)));
    }

    @SuppressWarnings("unused")
    public static class Person implements Serializable {

        private LocalDate birthday;

        public Person() {
        }

        private Person(LocalDate birthday) {
            this.birthday = birthday;
        }

        public LocalDate getBirthday() {
            return birthday;
        }

        public void setBirthday(LocalDate birthday) {
            this.birthday = birthday;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "birthday=" + birthday +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Person person = (Person) o;
            return Objects.equals(birthday, person.birthday);
        }

        @Override
        public int hashCode() {
            return Objects.hash(birthday);
        }
    }
}
