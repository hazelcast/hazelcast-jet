package com.hazelcast.jet.sql.impl.type.converter;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;

import static com.hazelcast.sql.impl.type.converter.Converters.getConverter;

@FunctionalInterface
public interface ToConverter {

    Object convert(Object value);

    class ToCharacterConverter implements ToConverter {

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

    class ToBigIntegerConverter implements ToConverter {

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

    class ToDateConverter implements ToConverter {

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

    class ToInstantConverter implements ToConverter {

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

    class ToZonedDateTimeConverter implements ToConverter {

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

    class ToCalendarConverter implements ToConverter {

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
}
