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

        converters.put(QueryDataType.BOOLEAN, new ToCanonicalConverter(QueryDataType.BOOLEAN));
        converters.put(QueryDataType.TINYINT, new ToCanonicalConverter(QueryDataType.TINYINT));
        converters.put(QueryDataType.SMALLINT, new ToCanonicalConverter(QueryDataType.SMALLINT));
        converters.put(QueryDataType.INT, new ToCanonicalConverter(QueryDataType.INT));
        converters.put(QueryDataType.BIGINT, new ToCanonicalConverter(QueryDataType.BIGINT));
        converters.put(QueryDataType.REAL, new ToCanonicalConverter(QueryDataType.REAL));
        converters.put(QueryDataType.DOUBLE, new ToCanonicalConverter(QueryDataType.DOUBLE));
        converters.put(QueryDataType.DECIMAL, new ToCanonicalConverter(QueryDataType.DECIMAL));
        converters.put(QueryDataType.DECIMAL_BIG_INTEGER, ToDecimalBigIntegerConverter.INSTANCE);

        converters.put(QueryDataType.VARCHAR_CHARACTER, ToVarcharCharacterConverter.INSTANCE);
        converters.put(QueryDataType.VARCHAR, new ToCanonicalConverter(QueryDataType.VARCHAR));

        converters.put(QueryDataType.TIME, new ToCanonicalConverter(QueryDataType.TIME));
        converters.put(QueryDataType.DATE, new ToCanonicalConverter(QueryDataType.DATE));
        converters.put(QueryDataType.TIMESTAMP, new ToCanonicalConverter(QueryDataType.TIMESTAMP));
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, ToTimestampWithTzZonedDateTimeConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, new ToCanonicalConverter(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME));
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_DATE, ToTimestampWithTzDateConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, ToTimestampWithTzInstantConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, ToTimestampWithTzCalendarConverter.INSTANCE);

        converters.put(QueryDataType.INTERVAL_YEAR_MONTH, new ToCanonicalConverter(QueryDataType.INTERVAL_YEAR_MONTH));
        converters.put(QueryDataType.INTERVAL_DAY_SECOND, new ToCanonicalConverter(QueryDataType.INTERVAL_DAY_SECOND));

        converters.put(QueryDataType.OBJECT, new ToCanonicalConverter(QueryDataType.OBJECT));

        return converters;
    }

    /**
     * Returns a {@code ToConverter} that converts to the canonical class of
     * the given {@code QueryDataType}.
     */
    private static class ToCanonicalConverter extends ToConverter {

        private ToCanonicalConverter(QueryDataType type) {
            super(type);
        }

        @Override
        public Object from(Object canonicalValue) {
            return canonicalValue;
        }
    }

    private static class ToVarcharCharacterConverter extends ToConverter {

        private static final ToVarcharCharacterConverter INSTANCE = new ToVarcharCharacterConverter();

        private ToVarcharCharacterConverter() {
            super(QueryDataType.VARCHAR_CHARACTER);
        }

        @Override
        public Object from(Object canonicalValue) {
            return ((String) canonicalValue).charAt(0);
        }
    }

    private static class ToDecimalBigIntegerConverter extends ToConverter {

        private static final ToDecimalBigIntegerConverter INSTANCE = new ToDecimalBigIntegerConverter();

        private ToDecimalBigIntegerConverter() {
            super(QueryDataType.DECIMAL_BIG_INTEGER);
        }

        @Override
        public Object from(Object canonicalValue) {
            return ((BigDecimal) canonicalValue).toBigInteger();
        }
    }

    private static class ToTimestampWithTzDateConverter extends ToConverter {

        private static final ToTimestampWithTzDateConverter INSTANCE = new ToTimestampWithTzDateConverter();

        private ToTimestampWithTzDateConverter() {
            super(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);
        }

        @Override
        public Object from(Object canonicalValue) {
            Instant instant = ((OffsetDateTime) canonicalValue).toInstant();
            return Date.from(instant);
        }
    }

    private static class ToTimestampWithTzInstantConverter extends ToConverter {

        private static final ToTimestampWithTzInstantConverter INSTANCE = new ToTimestampWithTzInstantConverter();

        private ToTimestampWithTzInstantConverter() {
            super(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT);
        }

        @Override
        public Object from(Object canonicalValue) {
            return ((OffsetDateTime) canonicalValue).toInstant();
        }
    }

    private static class ToTimestampWithTzZonedDateTimeConverter extends ToConverter {

        private static final ToTimestampWithTzZonedDateTimeConverter INSTANCE = new ToTimestampWithTzZonedDateTimeConverter();

        private ToTimestampWithTzZonedDateTimeConverter() {
            super(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME);
        }

        @Override
        public Object from(Object canonicalValue) {
            return ((OffsetDateTime) canonicalValue).toZonedDateTime();
        }
    }

    private static class ToTimestampWithTzCalendarConverter extends ToConverter {

        private static final ToTimestampWithTzCalendarConverter INSTANCE = new ToTimestampWithTzCalendarConverter();

        private ToTimestampWithTzCalendarConverter() {
            super(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR);
        }

        @Override
        public Object from(Object canonicalValue) {
            ZonedDateTime zdt = ((OffsetDateTime) canonicalValue).toZonedDateTime();
            return GregorianCalendar.from(zdt); // TODO: support other calendar types ?
        }
    }
}
