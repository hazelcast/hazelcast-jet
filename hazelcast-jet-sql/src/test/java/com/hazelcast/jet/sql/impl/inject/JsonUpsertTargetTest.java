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

package com.hazelcast.jet.sql.impl.inject;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.GregorianCalendar;

import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class JsonUpsertTargetTest {

    @Test
    public void test_set() {
        UpsertTarget target = new JsonUpsertTarget();
        UpsertInjector nullInjector = target.createInjector("null");
        UpsertInjector stringInjector = target.createInjector("string");
        UpsertInjector characterInjector = target.createInjector("character");
        UpsertInjector booleanInjector = target.createInjector("boolean");
        UpsertInjector byteInjector = target.createInjector("byte");
        UpsertInjector shortInjector = target.createInjector("short");
        UpsertInjector intInjector = target.createInjector("int");
        UpsertInjector longInjector = target.createInjector("long");
        UpsertInjector floatInjector = target.createInjector("float");
        UpsertInjector doubleInjector = target.createInjector("double");
        UpsertInjector bigDecimalInjector = target.createInjector("bigDecimal");
        UpsertInjector bigIntegerInjector = target.createInjector("bigInteger");
        UpsertInjector localTimeInjector = target.createInjector("localTime");
        UpsertInjector localDateInjector = target.createInjector("localDate");
        UpsertInjector localDateTimeInjector = target.createInjector("localDateTime");
        UpsertInjector dateInjector = target.createInjector("date");
        UpsertInjector calendarInjector = target.createInjector("calendar");
        UpsertInjector instantInjector = target.createInjector("instant");
        UpsertInjector zonedDatetimeInjector = target.createInjector("zonedDateTime");
        UpsertInjector offsetDateTimeInjector = target.createInjector("offsetDateTime");

        target.init();
        nullInjector.set(null);
        stringInjector.set("string");
        characterInjector.set('a');
        booleanInjector.set(true);
        byteInjector.set((byte) 127);
        shortInjector.set((short) 32767);
        intInjector.set(2147483647);
        longInjector.set(9223372036854775807L);
        floatInjector.set(1234567890.1F);
        doubleInjector.set(123451234567890.1D);
        bigDecimalInjector.set(new BigDecimal("9223372036854775.123"));
        bigIntegerInjector.set(new BigInteger("9223372036854775222"));
        localTimeInjector.set(LocalTime.of(12, 23, 34));
        localDateInjector.set(LocalDate.of(2020, 9, 9));
        localDateTimeInjector.set(LocalDateTime.of(2020, 9, 9, 12, 23, 34, 100_000_000));
        dateInjector.set(Date.from(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC).toInstant()));
        calendarInjector.set(GregorianCalendar.from(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 300_000_000, UTC)
                                                                  .toZonedDateTime()));
        instantInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 400_000_000, UTC).toInstant());
        zonedDatetimeInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 500_000_000, UTC).toZonedDateTime());
        offsetDateTimeInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 600_000_000, UTC));
        Object json = target.conclude();

        assertThat(json).isEqualTo("{"
                + "\"null\":null"
                + ",\"string\":\"string\""
                + ",\"character\":\"a\""
                + ",\"boolean\":true"
                + ",\"byte\":127"
                + ",\"short\":32767"
                + ",\"int\":2147483647"
                + ",\"long\":9223372036854775807"
                + ",\"float\":1.23456794E9"
                + ",\"double\":1.234512345678901E14"
                + ",\"bigDecimal\":\"9223372036854775.123\""
                + ",\"bigInteger\":\"9223372036854775222\""
                + ",\"localTime\":\"12:23:34\""
                + ",\"localDate\":\"2020-09-09\""
                + ",\"localDateTime\":\"2020-09-09T12:23:34.100\""
                + ",\"date\":\"" + OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC)
                                                 .atZoneSameInstant(localOffset()) + "\""
                + ",\"calendar\":\"2020-09-09T12:23:34.300Z\""
                + ",\"instant\":\"" + OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 400_000_000, UTC)
                                                    .atZoneSameInstant(localOffset()) + "\""
                + ",\"zonedDateTime\":\"2020-09-09T12:23:34.500Z\""
                + ",\"offsetDateTime\":\"2020-09-09T12:23:34.600Z\""
                + "}"
        );
    }

    private static ZoneOffset localOffset() {
        return systemDefault().getRules().getOffset(LocalDateTime.now());
    }
}
