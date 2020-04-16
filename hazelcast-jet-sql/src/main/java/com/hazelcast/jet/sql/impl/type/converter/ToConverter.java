package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

/**
 * Complementary interface to {@link Converter} that converts values back to
 * the class returned by {@link Converter#getValueClass()}.
 */
public abstract class ToConverter {

    protected final Converter converter;

    protected ToConverter(QueryDataType type) {
        this.converter = type.getConverter();
    }

    public Object convert(Object value) {
        if (value == null || converter.getValueClass() == value.getClass()) {
            return value;
        }
        Converter valueConverter = Converters.getConverter(value.getClass());
        return from(converter.convertToSelf(valueConverter, value));
    }

    protected abstract Object from(Object canonicalValue);
}
