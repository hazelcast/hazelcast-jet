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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class JsonUpsertTargetTest {

    @Test
    public void test_set() {
        UpsertTarget target = new JsonUpsertTarget();
        UpsertInjector nullInjector = target.createInjector("null");
        UpsertInjector stringInjector = target.createInjector("string");
        UpsertInjector booleanInjector = target.createInjector("boolean");
        UpsertInjector byteInjector = target.createInjector("byte");
        UpsertInjector shortInjector = target.createInjector("short");
        UpsertInjector intInjector = target.createInjector("int");
        UpsertInjector longInjector = target.createInjector("long");
        UpsertInjector floatInjector = target.createInjector("float");
        UpsertInjector doubleInjector = target.createInjector("double");
        UpsertInjector decimalInjector = target.createInjector("decimal");
        UpsertInjector timeInjector = target.createInjector("time");
        UpsertInjector dateInjector = target.createInjector("date");
        UpsertInjector timestampInjector = target.createInjector("timestamp");
        UpsertInjector timestampTzInjector = target.createInjector("timestampTz");

        target.init();
        nullInjector.set(null);
        stringInjector.set("string");
        booleanInjector.set(true);
        byteInjector.set((byte) 127);
        shortInjector.set((short) 32767);
        intInjector.set(2147483647);
        longInjector.set(9223372036854775807L);
        floatInjector.set(1234567890.1F);
        doubleInjector.set(123451234567890.1D);
        decimalInjector.set(new BigDecimal("9223372036854775.123"));
        timeInjector.set(LocalTime.of(12, 23, 34));
        dateInjector.set(LocalDate.of(2020, 9, 9));
        timestampInjector.set(LocalDateTime.of(2020, 9, 9, 12, 23, 34, 100_000_000));
        timestampTzInjector.set(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC));
        Object json = target.conclude();

        assertThat(json).isEqualTo("{"
                + "\"null\":null"
                + ",\"string\":\"string\""
                + ",\"boolean\":true"
                + ",\"byte\":127"
                + ",\"short\":32767"
                + ",\"int\":2147483647"
                + ",\"long\":9223372036854775807"
                + ",\"float\":1.23456794E9"
                + ",\"double\":1.234512345678901E14"
                + ",\"decimal\":\"9223372036854775.123\""
                + ",\"time\":\"12:23:34\""
                + ",\"date\":\"2020-09-09\""
                + ",\"timestamp\":\"2020-09-09T12:23:34.100\""
                + ",\"timestampTz\":\"2020-09-09T12:23:34.200Z\""
                + "}"
        );
    }
}
