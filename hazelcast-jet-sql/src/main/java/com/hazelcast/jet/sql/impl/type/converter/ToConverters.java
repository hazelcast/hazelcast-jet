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

package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class ToConverters {

    private static final Map<QueryDataType, ToConverter> CONVERTERS = prepareConverters();

    private ToConverters() {
    }

    @Nonnull
    public static ToConverter getToConverter(QueryDataType type) {
        return Objects.requireNonNull(CONVERTERS.get(type), "missing converter for " + type);
    }

    private static Map<QueryDataType, ToConverter> prepareConverters() {
        Map<QueryDataType, ToConverter> converters = new HashMap<>();

        converters.put(QueryDataType.BOOLEAN, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.TINYINT, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.SMALLINT, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.INT, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.BIGINT, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.REAL, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.DOUBLE, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.DECIMAL, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.DECIMAL_BIG_INTEGER, ToDecimalBigIntegerConverter.INSTANCE);

        converters.put(QueryDataType.VARCHAR_CHARACTER, ToVarcharCharacterConverter.INSTANCE);
        converters.put(QueryDataType.VARCHAR, ToCanonicalConverter.INSTANCE);

        converters.put(QueryDataType.TIME, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.DATE, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME,
                ToTimestampWithTzZonedDateTimeConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_DATE, ToTimestampWithTzDateConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, ToTimestampWithTzInstantConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, ToTimestampWithTzCalendarConverter.INSTANCE);

        converters.put(QueryDataType.INTERVAL_YEAR_MONTH, ToCanonicalConverter.INSTANCE);
        converters.put(QueryDataType.INTERVAL_DAY_SECOND, ToCanonicalConverter.INSTANCE);

        converters.put(QueryDataType.OBJECT, ToCanonicalConverter.INSTANCE);

        return converters;
    }

    /**
     * Returns a {@code ToConverter} that converts to the canonical class of
     * the given {@code QueryDataType}.
     */
    private static final class ToCanonicalConverter implements ToConverter {

        private static final ToCanonicalConverter INSTANCE = new ToCanonicalConverter();

        private ToCanonicalConverter() {
        }

        @Override
        public Object convert(Object canonicalValue) {
            return canonicalValue;
        }
    }

    private static final class ToVarcharCharacterConverter implements ToConverter {

        private static final ToVarcharCharacterConverter INSTANCE = new ToVarcharCharacterConverter();

        private ToVarcharCharacterConverter() {
        }

        @Override
        public Object convert(Object canonicalValue) {
            return canonicalValue == null ? null : ((String) canonicalValue).charAt(0);
        }
    }

    private static final class ToDecimalBigIntegerConverter implements ToConverter {

        private static final ToDecimalBigIntegerConverter INSTANCE = new ToDecimalBigIntegerConverter();

        private ToDecimalBigIntegerConverter() {
        }

        @Override
        public Object convert(Object canonicalValue) {
            return canonicalValue == null ? null : ((BigDecimal) canonicalValue).toBigInteger();
        }
    }

    private static final class ToTimestampWithTzDateConverter implements ToConverter {

        private static final ToTimestampWithTzDateConverter INSTANCE = new ToTimestampWithTzDateConverter();

        private ToTimestampWithTzDateConverter() {
        }

        @Override
        public Object convert(Object canonicalValue) {
            if (canonicalValue == null) {
                return null;
            } else {
                Instant instant = ((OffsetDateTime) canonicalValue).toInstant();
                return Date.from(instant);
            }
        }
    }

    private static final class ToTimestampWithTzCalendarConverter implements ToConverter {

        private static final ToTimestampWithTzCalendarConverter INSTANCE = new ToTimestampWithTzCalendarConverter();

        private ToTimestampWithTzCalendarConverter() {
        }

        @Override
        public Object convert(Object canonicalValue) {
            if (canonicalValue == null) {
                return null;
            } else {
                ZonedDateTime zdt = ((OffsetDateTime) canonicalValue).toZonedDateTime();
                return GregorianCalendar.from(zdt); // TODO: support other calendar types ?
            }
        }
    }

    private static final class ToTimestampWithTzInstantConverter implements ToConverter {

        private static final ToTimestampWithTzInstantConverter INSTANCE = new ToTimestampWithTzInstantConverter();

        private ToTimestampWithTzInstantConverter() {
        }

        @Override
        public Object convert(Object canonicalValue) {
            return canonicalValue == null ? null : ((OffsetDateTime) canonicalValue).toInstant();
        }
    }

    private static final class ToTimestampWithTzZonedDateTimeConverter implements ToConverter {

        private static final ToTimestampWithTzZonedDateTimeConverter INSTANCE =
                new ToTimestampWithTzZonedDateTimeConverter();

        private ToTimestampWithTzZonedDateTimeConverter() {
        }

        @Override
        public Object convert(Object canonicalValue) {
            return canonicalValue == null ? null : ((OffsetDateTime) canonicalValue).toZonedDateTime();
        }
    }
}
