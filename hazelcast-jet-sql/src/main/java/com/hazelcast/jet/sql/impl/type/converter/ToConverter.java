package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.util.Calendar;

/**
 * Complementary interface to {@link Converter} that converts values back to
 * the class returned by {@link Converter#getValueClass()}.
 */
public abstract class ToConverter {

    private final Converter converter;

    protected ToConverter(QueryDataType type) {
        this.converter = type.getConverter();
    }

    public Object convert(Object value) {
        Class<?> valueClass = converter.getValueClass();
        if (value == null ||
                valueClass == value.getClass() ||
                (valueClass == Calendar.class && value instanceof Calendar)) {
            return value;
        }
        Converter valueConverter = Converters.getConverter(value.getClass());
        return from(converter.convertToSelf(valueConverter, value));
    }

    protected abstract Object from(Object canonicalValue);
}
