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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
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

public class AvroUpsertTargetTest {

    @Test
    public void test_set() {
        Schema schema = SchemaBuilder.record("name")
                                     .fields()
                                     .name("null").type().nullable().record("nested").fields().endRecord().noDefault()
                                     .name("string").type().stringType().noDefault()
                                     .name("character").type().stringType().noDefault()
                                     .name("boolean").type().booleanType().noDefault()
                                     .name("byte").type().intType().noDefault()
                                     .name("short").type().intType().noDefault()
                                     .name("int").type().intType().noDefault()
                                     .name("long").type().longType().noDefault()
                                     .name("float").type().floatType().noDefault()
                                     .name("double").type().doubleType().noDefault()
                                     .name("bigDecimal").type().stringType().noDefault()
                                     .name("bigInteger").type().stringType().noDefault()
                                     .name("localTime").type().stringType().noDefault()
                                     .name("localDate").type().stringType().noDefault()
                                     .name("localDateTime").type().stringType().noDefault()
                                     .name("date").type().stringType().noDefault()
                                     .name("calendar").type().stringType().noDefault()
                                     .name("instant").type().stringType().noDefault()
                                     .name("zonedDateTime").type().stringType().noDefault()
                                     .name("offsetDateTime").type().stringType().noDefault()
                                     .endRecord();

        UpsertTarget target = new AvroUpsertTarget(schema.toString());
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
        UpsertInjector offsetDatetimeInjector = target.createInjector("offsetDateTime");

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
        calendarInjector.set(
                GregorianCalendar.from(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 300_000_000, UTC).toZonedDateTime())
        );
        instantInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 400_000_000, UTC).toInstant());
        zonedDatetimeInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 500_000_000, UTC).toZonedDateTime());
        offsetDatetimeInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 600_000_000, UTC));
        Object record = target.conclude();

        assertThat(record).isEqualTo(new GenericRecordBuilder(schema)
                .set("null", null)
                .set("string", "string")
                .set("character", "a")
                .set("boolean", true)
                .set("byte", (byte) 127)
                .set("short", (short) 32767)
                .set("int", 2147483647)
                .set("long", 9223372036854775807L)
                .set("float", 1234567890.1F)
                .set("double", 123451234567890.1D)
                .set("bigDecimal", "9223372036854775.123")
                .set("bigInteger", "9223372036854775222")
                .set("localTime", "12:23:34")
                .set("localDate", "2020-09-09")
                .set("localDateTime", "2020-09-09T12:23:34.100")
                .set("date", OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC)
                                           .atZoneSameInstant(localOffset())
                                           .toString())
                .set("calendar", "2020-09-09T12:23:34.300Z")
                .set("instant", OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 400_000_000, UTC)
                                              .atZoneSameInstant(localOffset())
                                              .toString())
                .set("zonedDateTime", "2020-09-09T12:23:34.500Z")
                .set("offsetDateTime", "2020-09-09T12:23:34.600Z")
                .build()
        );
    }

    private static ZoneOffset localOffset() {
        return systemDefault().getRules().getOffset(LocalDateTime.now());
    }
}
