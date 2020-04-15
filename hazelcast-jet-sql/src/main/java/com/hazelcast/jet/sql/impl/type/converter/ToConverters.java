package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.sql.impl.type.converter.Converters.getConverter;

public final class ToConverters {

    private static final Map<String, ToConverter> CONVERTERS = prepareConverters();

    private ToConverters() {
    }

    @Nonnull
    public static ToConverter getToConverter(String name) {
        return Objects.requireNonNull(CONVERTERS.get(name), "missing converter for " + name);
    }

    private static Map<String, ToConverter> prepareConverters() {
        Map<String, ToConverter> converters = new HashMap<>();

        converters.put(Boolean.class.getName(), canonicalConverter(QueryDataType.BOOLEAN));
        converters.put(boolean.class.getName(), canonicalConverter(QueryDataType.BOOLEAN));

        converters.put(Byte.class.getName(), canonicalConverter(QueryDataType.TINYINT));
        converters.put(byte.class.getName(), canonicalConverter(QueryDataType.TINYINT));

        converters.put(Short.class.getName(), canonicalConverter(QueryDataType.SMALLINT));
        converters.put(short.class.getName(), canonicalConverter(QueryDataType.SMALLINT));

        converters.put(Integer.class.getName(), canonicalConverter(QueryDataType.INT));
        converters.put(int.class.getName(), canonicalConverter(QueryDataType.INT));

        converters.put(Long.class.getName(), canonicalConverter(QueryDataType.BIGINT));
        converters.put(long.class.getName(), canonicalConverter(QueryDataType.BIGINT));

        converters.put(Float.class.getName(), canonicalConverter(QueryDataType.REAL));
        converters.put(float.class.getName(), canonicalConverter(QueryDataType.REAL));

        converters.put(Double.class.getName(), canonicalConverter(QueryDataType.DOUBLE));
        converters.put(double.class.getName(), canonicalConverter(QueryDataType.DOUBLE));

        converters.put(BigDecimal.class.getName(), canonicalConverter(QueryDataType.DECIMAL));
        converters.put(BigInteger.class.getName(), ToDecimalBigIntegerConverter.INSTANCE);

        converters.put(Character.class.getName(), ToVarcharCharacterConverter.INSTANCE);
        converters.put(char.class.getName(), ToVarcharCharacterConverter.INSTANCE);

        converters.put(String.class.getName(), canonicalConverter(QueryDataType.VARCHAR));

        converters.put(Date.class.getName(), ToTimestampWithTzDateConverter.INSTANCE);
        converters.put(Instant.class.getName(), ToTimestampWithTzInstantConverter.INSTANCE);
        converters.put(LocalTime.class.getName(), canonicalConverter(QueryDataType.TIME));
        converters.put(LocalDate.class.getName(), canonicalConverter(QueryDataType.DATE));
        converters.put(LocalDateTime.class.getName(), canonicalConverter(QueryDataType.TIMESTAMP));
        converters.put(GregorianCalendar.class.getName(), ToTimestampWithTzCalendarConverter.INSTANCE);
        converters.put(ZonedDateTime.class.getName(), ToTimestampWithTzZonedDateTimeConverter.INSTANCE);
        converters.put(OffsetDateTime.class.getName(), canonicalConverter(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME));
        converters.put(SqlYearMonthInterval.class.getName(), canonicalConverter(QueryDataType.INTERVAL_YEAR_MONTH));
        converters.put(SqlDaySecondInterval.class.getName(), canonicalConverter(QueryDataType.INTERVAL_DAY_SECOND));

        converters.put(Object.class.getName(), canonicalConverter(QueryDataType.OBJECT));

        return converters;
    }

    private static class ToVarcharCharacterConverter implements ToConverter {

        public static final ToVarcharCharacterConverter INSTANCE = new ToVarcharCharacterConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == Character.class) {
                return value;
            }
            return getConverter(value.getClass())
                    .asVarchar(value)
                    .charAt(0);
        }
    }

    private static class ToDecimalBigIntegerConverter implements ToConverter {

        public static final ToDecimalBigIntegerConverter INSTANCE = new ToDecimalBigIntegerConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == BigInteger.class) {
                return value;
            }
            return getConverter(value.getClass())
                    .asDecimal(value)
                    .toBigInteger();
        }
    }

    private static class ToTimestampWithTzDateConverter implements ToConverter {

        public static final ToTimestampWithTzDateConverter INSTANCE = new ToTimestampWithTzDateConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == Date.class) {
                return value;
            }

            Instant instant = getConverter(value.getClass())
                    .asTimestampWithTimezone(value)
                    .toInstant();
            return Date.from(instant);
        }
    }

    private static class ToTimestampWithTzInstantConverter implements ToConverter {

        public static final ToTimestampWithTzInstantConverter INSTANCE = new ToTimestampWithTzInstantConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == Instant.class) {
                return value;
            }
            return getConverter(value.getClass())
                    .asTimestampWithTimezone(value)
                    .toInstant();
        }
    }

    private static class ToTimestampWithTzZonedDateTimeConverter implements ToConverter {

        public static final ToTimestampWithTzZonedDateTimeConverter INSTANCE = new ToTimestampWithTzZonedDateTimeConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == ZonedDateTime.class) {
                return value;
            }
            return getConverter(value.getClass())
                    .asTimestampWithTimezone(value)
                    .toZonedDateTime();
        }
    }

    private static class ToTimestampWithTzCalendarConverter implements ToConverter {

        public static final ToTimestampWithTzCalendarConverter INSTANCE = new ToTimestampWithTzCalendarConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == GregorianCalendar.class) {
                return value;
            }
            ZonedDateTime zdt = getConverter(value.getClass())
                    .asTimestampWithTimezone(value)
                    .toZonedDateTime();
            return GregorianCalendar.from(zdt);
        }
    }

    /**
     * Returns a {@code ToConverter} that converts to the canonical class of
     * the given {@code QueryDataType}.
     */
    private static ToConverter canonicalConverter(QueryDataType type) {
        Converter typeConverter = type.getConverter();
        return value -> {
            if (value == null || typeConverter.getValueClass() == value.getClass()) {
                return value;
            }
            Converter converter = Converters.getConverter(value.getClass());
            return typeConverter.convertToSelf(converter, value);
        };
    }
}
