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
        UpsertInjector offsetDatetimeInjector = target.createInjector("offsetDatetime");

        target.init();
        nullInjector.set(null);
        stringInjector.set("a");
        characterInjector.set('b');
        booleanInjector.set(true);
        byteInjector.set((byte) 1);
        shortInjector.set((short) 2);
        intInjector.set(3);
        longInjector.set(4L);
        floatInjector.set(5.1F);
        doubleInjector.set(5.2D);
        bigDecimalInjector.set(new BigDecimal("6.1"));
        bigIntegerInjector.set(new BigInteger("7"));
        localTimeInjector.set(LocalTime.of(12, 23, 34));
        localDateInjector.set(LocalDate.of(2020, 9, 9));
        localDateTimeInjector.set(LocalDateTime.of(2020, 9, 9, 12, 23, 34, 100_000_000));
        dateInjector.set(Date.from(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC).toInstant()));
        calendarInjector.set(
                GregorianCalendar.from(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 300_000_000, UTC).toZonedDateTime())
        );
        instantInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 400_000_000, UTC).toInstant());
        zonedDatetimeInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 500_000_000, UTC).toZonedDateTime());
        offsetDatetimeInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 600_000_000, UTC));
        Object value = target.conclude();

        assertThat(value).isEqualTo("{"
                + "\"null\":null"
                + ",\"string\":\"a\""
                + ",\"character\":\"b\""
                + ",\"boolean\":true"
                + ",\"byte\":1"
                + ",\"short\":2"
                + ",\"int\":3"
                + ",\"long\":4"
                + ",\"float\":5.1"
                + ",\"double\":5.2"
                + ",\"bigDecimal\":\"6.1\""
                + ",\"bigInteger\":\"7\""
                + ",\"localTime\":\"12:23:34\""
                + ",\"localDate\":\"2020-09-09\""
                + ",\"localDateTime\":\"2020-09-09T12:23:34.100\""
                + ",\"date\":\"" +
                OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC).atZoneSameInstant(localOffset()) + "\""
                + ",\"calendar\":\"2020-09-09T12:23:34.300Z\""
                + ",\"instant\":\"" +
                OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 400_000_000, UTC).atZoneSameInstant(localOffset()) + "\""
                + ",\"zonedDateTime\":\"2020-09-09T12:23:34.500Z\""
                + ",\"offsetDatetime\":\"2020-09-09T12:23:34.600Z\""
                + "}"
        );
    }

    private static ZoneOffset localOffset() {
        return systemDefault().getRules().getOffset(LocalDateTime.now());
    }
}
