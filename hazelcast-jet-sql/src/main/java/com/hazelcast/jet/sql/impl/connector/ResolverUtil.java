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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.sql.impl.QueryException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.Character.toLowerCase;
import static java.lang.Character.toUpperCase;

// TODO: move it to ReflectionUtil ?
public final class ResolverUtil {

    private static final String METHOD_PREFIX_GET = "get";
    private static final String METHOD_PREFIX_IS = "is";
    private static final String METHOD_PREFIX_SET = "set";

    private static final String METHOD_GET_FACTORY_ID = "getFactoryId";
    private static final String METHOD_GET_CLASS_ID = "getClassId";
    private static final String METHOD_GET_CLASS_VERSION = "getVersion";

    private ResolverUtil() {
    }

    public static Map<String, Class<?>> resolveClass(Class<?> clazz) {
        Map<String, Class<?>> fields = new LinkedHashMap<>();

        for (Method method : clazz.getMethods()) {
            BiTuple<String, Class<?>> property = extractProperty(clazz, method);
            if (property == null) {
                continue;
            }
            fields.putIfAbsent(property.element1(), property.element2());
        }

        Class<?> classToInspect = clazz;
        while (classToInspect != Object.class) {
            for (Field field : classToInspect.getDeclaredFields()) {
                if (skipField(field)) {
                    continue;
                }
                fields.putIfAbsent(field.getName(), field.getType());
            }
            classToInspect = classToInspect.getSuperclass();
        }

        return fields;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:NPathComplexity"})
    private static BiTuple<String, Class<?>> extractProperty(Class<?> clazz, Method method) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return null;
        }

        if (Modifier.isStatic(method.getModifiers())) {
            return null;
        }

        Class<?> returnType = method.getReturnType();
        if (returnType == void.class || returnType == Void.class) {
            return null;
        }

        if (method.getParameterCount() != 0) {
            return null;
        }

        if (method.getDeclaringClass() == Object.class) {
            return null;
        }

        String methodName = method.getName();
        if (methodName.equals(METHOD_GET_FACTORY_ID)
                || methodName.equals(METHOD_GET_CLASS_ID)
                || methodName.equals(METHOD_GET_CLASS_VERSION)) {
            if (IdentifiedDataSerializable.class.isAssignableFrom(clazz) || Portable.class.isAssignableFrom(clazz)) {
                return null;
            }
        }

        String propertyName = extractPropertyName(method);
        if (propertyName == null) {
            return null;
        }

        Class<?> propertyClass = method.getReturnType();
        if (extractSetter(clazz, propertyName, propertyClass) == null) {
            return null;
        }

        return BiTuple.of(propertyName, propertyClass);
    }

    private static String extractPropertyName(Method method) {
        String fieldNameWithWrongCase;

        String methodName = method.getName();
        if (methodName.startsWith(METHOD_PREFIX_GET) && methodName.length() > METHOD_PREFIX_GET.length()) {
            fieldNameWithWrongCase = methodName.substring(METHOD_PREFIX_GET.length());
        } else if (methodName.startsWith(METHOD_PREFIX_IS) && methodName.length() > METHOD_PREFIX_IS.length()) {
            // Skip getters that do not return primitive boolean.
            if (method.getReturnType() != boolean.class) {
                return null;
            }

            fieldNameWithWrongCase = methodName.substring(METHOD_PREFIX_IS.length());
        } else {
            return null;
        }

        return toLowerCase(fieldNameWithWrongCase.charAt(0)) + fieldNameWithWrongCase.substring(1);
    }

    public static Method extractSetter(Class<?> clazz, String propertyName, Class<?> type) {
        String setName = METHOD_PREFIX_SET + toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);

        Method method;
        try {
            method = clazz.getMethod(setName, type);
        } catch (NoSuchMethodException e) {
            return null;
        }

        if (!isSetter(method)) {
            return null;
        }

        return method;
    }

    @SuppressWarnings("RedundantIfStatement")
    private static boolean isSetter(Method method) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return false;
        }

        if (Modifier.isStatic(method.getModifiers())) {
            return false;
        }

        Class<?> returnType = method.getReturnType();
        if (returnType != void.class && returnType != Void.class) {
            return false;
        }

        return true;
    }

    public static Field extractField(Class<?> clazz, String fieldName) {
        Field field;
        try {
            field = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            return null;
        }

        if (skipField(field)) {
            return null;
        }

        return field;
    }

    @SuppressWarnings("RedundantIfStatement")
    private static boolean skipField(Field field) {
        if (!Modifier.isPublic(field.getModifiers())) {
            return true;
        }

        return false;
    }

    public static ClassDefinition lookupClassDefinition(
            InternalSerializationService serializationService,
            int factoryId,
            int classId,
            int classVersion
    ) {
        ClassDefinition classDefinition = serializationService
                .getPortableContext()
                .lookupClassDefinition(factoryId, classId, classVersion);
        if (classDefinition == null) {
            throw QueryException.dataException(
                    "Unable to find class definition for factoryId: " + factoryId
                            + ", classId: " + classId + ", classVersion: " + classVersion
            );
        }
        return classDefinition;
    }
}
