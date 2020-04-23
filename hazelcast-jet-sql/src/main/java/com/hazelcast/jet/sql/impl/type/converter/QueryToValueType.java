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

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL_BIG_INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_DATE;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_INSTANT;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR_CHARACTER;

/**
 * Canonical/SQL type set is narrower ({@link com.hazelcast.jet.sql.impl.expression.SqlToQueryType})
 * than the set of types we support writing to.
 */
public final class QueryToValueType {

    private static final Map<Entry<QueryDataType, String>, QueryDataType> QUERY_TO_VALUE_TYPE = prepareMapping();

    private QueryToValueType() {
    }

    private static Map<Entry<QueryDataType, String>, QueryDataType> prepareMapping() {
        Map<Entry<QueryDataType, String>, QueryDataType> mapping = new HashMap<>();

        mapping.put(entry(VARCHAR, char.class.getName()), VARCHAR_CHARACTER);
        mapping.put(entry(VARCHAR, Character.class.getName()), VARCHAR_CHARACTER);

        mapping.put(entry(DECIMAL, BigInteger.class.getName()), DECIMAL_BIG_INTEGER);

        mapping.put(entry(TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, Date.class.getName()), TIMESTAMP_WITH_TZ_DATE);
        mapping.put(entry(TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, Calendar.class.getName()), TIMESTAMP_WITH_TZ_CALENDAR);
        mapping.put(entry(TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, GregorianCalendar.class.getName()), TIMESTAMP_WITH_TZ_CALENDAR); // TODO: support other calendar types ?
        mapping.put(entry(TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, Instant.class.getName()), TIMESTAMP_WITH_TZ_INSTANT);
        mapping.put(entry(TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, ZonedDateTime.class.getName()), TIMESTAMP_WITH_TZ_ZONED_DATE_TIME);

        return mapping;
    }

    public static QueryDataType map(QueryDataType canonicalType, String valueClassName) {
        return QUERY_TO_VALUE_TYPE.getOrDefault(entry(canonicalType, valueClassName), canonicalType);
    }
}
