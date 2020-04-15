package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.converter.Converter;

/**
 * Complementary interface to {@link Converter} that converts values back to
 * the class returned by {@link Converter#getValueClass()}.
 */
@FunctionalInterface
public interface ToConverter {

    Object convert(Object value);

}
