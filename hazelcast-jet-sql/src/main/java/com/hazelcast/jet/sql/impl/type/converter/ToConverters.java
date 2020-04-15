package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import com.hazelcast.sql.impl.type.converter.SqlDaySecondIntervalConverter;
import com.hazelcast.sql.impl.type.converter.SqlYearMonthIntervalConverter;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
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

        converters.put(Boolean.class.getName(), identityConverter(QueryDataType.BOOLEAN));
        converters.put(boolean.class.getName(), identityConverter(QueryDataType.BOOLEAN));

        converters.put(Byte.class.getName(), identityConverter(QueryDataType.TINYINT));
        converters.put(byte.class.getName(), identityConverter(QueryDataType.TINYINT));

        converters.put(Short.class.getName(), identityConverter(QueryDataType.SMALLINT));
        converters.put(short.class.getName(), identityConverter(QueryDataType.SMALLINT));

        converters.put(Integer.class.getName(), identityConverter(QueryDataType.INT));
        converters.put(int.class.getName(), identityConverter(QueryDataType.INT));

        converters.put(Long.class.getName(), identityConverter(QueryDataType.BIGINT));
        converters.put(long.class.getName(), identityConverter(QueryDataType.BIGINT));

        converters.put(Float.class.getName(), identityConverter(QueryDataType.REAL));
        converters.put(float.class.getName(), identityConverter(QueryDataType.REAL));

        converters.put(Double.class.getName(), identityConverter(QueryDataType.DOUBLE));
        converters.put(double.class.getName(), identityConverter(QueryDataType.DOUBLE));

        converters.put(BigDecimal.class.getName(), identityConverter(QueryDataType.DECIMAL));
        converters.put(BigInteger.class.getName(), ToBigIntegerConverter.INSTANCE);

        converters.put(Character.class.getName(), ToCharacterConverter.INSTANCE);
        converters.put(char.class.getName(), ToCharacterConverter.INSTANCE);

        converters.put(String.class.getName(), identityConverter(QueryDataType.VARCHAR));

        converters.put(Date.class.getName(), ToDateConverter.INSTANCE);
        converters.put(Instant.class.getName(), ToInstantConverter.INSTANCE);
        converters.put(LocalTime.class.getName(), identityConverter(QueryDataType.TIME));
        converters.put(LocalDate.class.getName(), identityConverter(QueryDataType.DATE));
        converters.put(LocalDateTime.class.getName(), identityConverter(QueryDataType.TIMESTAMP));
        converters.put(Calendar.class.getName(), ToCalendarConverter.INSTANCE);
        converters.put(ZonedDateTime.class.getName(), ToZonedDateTimeConverter.INSTANCE);
        converters.put(OffsetDateTime.class.getName(), identityConverter(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME));
        converters.put(SqlYearMonthIntervalConverter.class.getName(), identityConverter(QueryDataType.INTERVAL_YEAR_MONTH));
        converters.put(SqlDaySecondIntervalConverter.class.getName(), identityConverter(QueryDataType.INTERVAL_DAY_SECOND));

        converters.put(Object.class.getName(), identityConverter(QueryDataType.OBJECT));

        return converters;
    }

    private static class ToCharacterConverter implements ToConverter {

        public static final ToCharacterConverter INSTANCE = new ToCharacterConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == Date.class) {
                return value;
            }
            return getConverter(value.getClass())
                    .asVarchar(value)
                    .charAt(0);
        }
    }

    private static class ToBigIntegerConverter implements ToConverter {

        public static final ToBigIntegerConverter INSTANCE = new ToBigIntegerConverter();

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

    private static class ToDateConverter implements ToConverter {

        public static final ToDateConverter INSTANCE = new ToDateConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == Date.class) {
                return value;
            }

            return Date.from(getConverter(value.getClass())
                    .asDate(value)
                    .atStartOfDay()
                    .atZone(ZoneId.systemDefault())
                    .toInstant());
        }
    }

    private static class ToInstantConverter implements ToConverter {

        public static final ToInstantConverter INSTANCE = new ToInstantConverter();

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

    private static class ToZonedDateTimeConverter implements ToConverter {

        public static final ToZonedDateTimeConverter INSTANCE = new ToZonedDateTimeConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == ZonedDateTime.class) {
                return value;
            }
            return getConverter(value.getClass())
                    .asTimestampWithTimezone(value)
                    .atZoneSameInstant(ZoneId.systemDefault());
        }
    }

    private static class ToCalendarConverter implements ToConverter {

        public static final ToCalendarConverter INSTANCE = new ToCalendarConverter();

        @Override
        public Object convert(Object value) {
            if (value == null || value.getClass() == GregorianCalendar.class) {
                return value;
            }
            return GregorianCalendar.from(getConverter(value.getClass())
                    .asTimestampWithTimezone(value)
                    .atZoneSameInstant(ZoneId.systemDefault()));
        }
    }

    private static ToConverter identityConverter(QueryDataType type) {
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
