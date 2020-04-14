package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.jet.sql.impl.type.converter.ToConverter.ToBigIntegerConverter;
import com.hazelcast.jet.sql.impl.type.converter.ToConverter.ToCalendarConverter;
import com.hazelcast.jet.sql.impl.type.converter.ToConverter.ToCharacterConverter;
import com.hazelcast.jet.sql.impl.type.converter.ToConverter.ToDateConverter;
import com.hazelcast.jet.sql.impl.type.converter.ToConverter.ToInstantConverter;
import com.hazelcast.jet.sql.impl.type.converter.ToConverter.ToZonedDateTimeConverter;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import com.hazelcast.sql.impl.type.converter.SqlDaySecondIntervalConverter;
import com.hazelcast.sql.impl.type.converter.SqlYearMonthIntervalConverter;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class ToConverters {

    private static final Map<String, ToConverter> CONVERTERS = prepareConverters();

    private ToConverters() {
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

    public static ToConverter identityConverter(QueryDataType type) {
        Converter typeConverter = type.getConverter();
        return value -> {
            if (value == null || typeConverter.getValueClass() == value.getClass()) {
                return value;
            }
            Converter converter = Converters.getConverter(value.getClass());
            return typeConverter.convertToSelf(converter, value);
        };
    }

    public static ToConverter getToConverter(String name) {
        return Objects.requireNonNull(CONVERTERS.get(name), "missing converter for " + name);
    }
}
