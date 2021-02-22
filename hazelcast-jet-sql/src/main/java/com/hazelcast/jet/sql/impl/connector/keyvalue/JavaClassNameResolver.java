/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public final class JavaClassNameResolver {

    private static final Map<String, String> CLASS_NAMES_BY_FORMAT = new HashMap<String, String>() {{
        put("varchar", String.class.getName());
        put("character varying", String.class.getName());
        put("char varying", String.class.getName());
        put("boolean", Boolean.class.getName());
        put("tinyint", Byte.class.getName());
        put("smallint", Short.class.getName());
        put("integer", Integer.class.getName());
        put("int", Integer.class.getName());
        put("bigint", Long.class.getName());
        put("decimal", BigDecimal.class.getName());
        put("dec", BigDecimal.class.getName());
        put("numeric", BigDecimal.class.getName());
        put("real", Float.class.getName());
        put("double", Double.class.getName());
        put("double precision", Double.class.getName());
        put("time", LocalTime.class.getName());
        put("date", LocalDate.class.getName());
        put("timestamp", LocalDateTime.class.getName());
        put("timestamp with time zone", OffsetDateTime.class.getName());
    }};

    private JavaClassNameResolver() {
    }

    public static String resolveClassName(String format) {
        return CLASS_NAMES_BY_FORMAT.get(format);
    }

    static Stream<String> formats() {
        return CLASS_NAMES_BY_FORMAT.keySet().stream();
    }
}
