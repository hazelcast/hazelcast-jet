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

package com.hazelcast.jet.impl.util;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.lang.Character.toLowerCase;
import static java.lang.Character.toUpperCase;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

public final class ReflectionUtils {

    private static final String METHOD_PREFIX_GET = "get";
    private static final String METHOD_PREFIX_IS = "is";
    private static final String METHOD_PREFIX_SET = "set";

    private static final String METHOD_GET_FACTORY_ID = "getFactoryId";
    private static final String METHOD_GET_CLASS_ID = "getClassId";
    private static final String METHOD_GET_CLASS_VERSION = "getVersion";

    private ReflectionUtils() {
    }

    public static Class<?> loadClass(String name) {
        return loadClass(Thread.currentThread().getContextClassLoader(), name);
    }

    public static Class<?> loadClass(ClassLoader classLoader, String name) {
        try {
            return ClassLoaderUtil.loadClass(classLoader, name);
        } catch (ClassNotFoundException e) {
            throw sneakyThrow(e);
        }
    }

    public static <T> T newInstance(ClassLoader classLoader, String name) {
        try {
            return ClassLoaderUtil.newInstance(classLoader, name);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    public static <T> T readStaticFieldOrNull(String className, String fieldName) {
        try {
            Class<?> clazz = Class.forName(className);
            return readStaticField(clazz, fieldName);
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException | SecurityException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T readStaticField(Class<?> clazz, String fieldName) throws NoSuchFieldException,
            IllegalAccessException {
        Field field = clazz.getDeclaredField(fieldName);
        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
        return (T) field.get(null);
    }

    public static Map<String, Class<?>> extractProperties(Class<?> clazz) {
        Map<String, Class<?>> properties = new LinkedHashMap<>();

        for (Method method : clazz.getMethods()) {
            BiTuple<String, Class<?>> property = extractProperty(clazz, method);
            if (property == null) {
                continue;
            }
            properties.putIfAbsent(property.element1(), property.element2());
        }

        Class<?> classToInspect = clazz;
        while (classToInspect != Object.class) {
            for (Field field : classToInspect.getDeclaredFields()) {
                if (skipField(field)) {
                    continue;
                }
                properties.putIfAbsent(field.getName(), field.getType());
            }
            classToInspect = classToInspect.getSuperclass();
        }

        return properties;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:NPathComplexity"})
    private static BiTuple<String, Class<?>> extractProperty(Class<?> clazz, Method method) {
        if (!isGetter(clazz, method)) {
            return null;
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

    @SuppressWarnings("RedundantIfStatement")
    private static boolean isGetter(Class<?> clazz, Method method) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return false;
        }

        if (Modifier.isStatic(method.getModifiers())) {
            return false;
        }

        Class<?> returnType = method.getReturnType();
        if (returnType == void.class || returnType == Void.class) {
            return false;
        }

        if (method.getParameterCount() != 0) {
            return false;
        }

        if (method.getDeclaringClass() == Object.class) {
            return false;
        }

        String methodName = method.getName();
        if (methodName.equals(METHOD_GET_FACTORY_ID)
                || methodName.equals(METHOD_GET_CLASS_ID)
                || methodName.equals(METHOD_GET_CLASS_VERSION)) {
            if (IdentifiedDataSerializable.class.isAssignableFrom(clazz) || Portable.class.isAssignableFrom(clazz)) {
                return false;
            }
        }

        return true;
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

    @Nonnull
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification =
            "False positive on try-with-resources as of JDK11")
    public static Collection<Class<?>> nestedClassesOf(Class<?>... classes) {
        ClassGraph classGraph = new ClassGraph()
                .enableClassInfo()
                .ignoreClassVisibility();
        stream(classes).map(Class::getClassLoader).distinct().forEach(classGraph::addClassLoader);
        stream(classes).map(ReflectionUtils::toPackageName).distinct().forEach(classGraph::whitelistPackages);
        try (ScanResult scanResult = classGraph.scan()) {
            Set<String> classNames = stream(classes).map(Class::getName).collect(toSet());
            return concat(
                    stream(classes),
                    scanResult.getAllClasses()
                              .stream()
                              .filter(classInfo -> classNames.contains(classInfo.getName()))
                              .flatMap(classInfo -> classInfo.getInnerClasses().stream())
                              .map(ClassInfo::loadClass)
            ).collect(toList());
        }
    }

    private static String toPackageName(Class<?> clazz) {
        return Optional.ofNullable(clazz.getPackage()).map(Package::getName).orElse("");
    }

    @Nonnull
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification =
            "False positive on try-with-resources as of JDK11")
    public static Resources resourcesOf(String... packages) {
        String[] paths = stream(packages).map(ReflectionUtils::toPath).toArray(String[]::new);
        ClassGraph classGraph = new ClassGraph()
                .whitelistPackages(packages)
                .whitelistPaths(paths)
                .ignoreClassVisibility();
        try (ScanResult scanResult = classGraph.scan()) {
            Collection<ClassResource> classes = Util.toList(scanResult.getAllClasses(), ClassResource::new);
            Collection<URL> nonClasses = scanResult.getAllResources().nonClassFilesOnly().getURLs();
            return new Resources(classes, nonClasses);
        }
    }

    private static String toPath(String name) {
        return name.replace('.', '/');
    }

    public static String toClassResourceId(String name) {
        return toPath(name) + ".class";
    }

    public static final class Resources {

        private final Collection<ClassResource> classes;
        private final Collection<URL> nonClasses;

        private Resources(Collection<ClassResource> classes, Collection<URL> nonClasses) {
            this.classes = classes;
            this.nonClasses = nonClasses;
        }

        public Stream<ClassResource> classes() {
            return classes.stream();
        }

        public Stream<URL> nonClasses() {
            return nonClasses.stream();
        }
    }

    public static final class ClassResource {

        private final String id;
        private final URL url;

        private ClassResource(ClassInfo classInfo) {
            this(classInfo.getName(), classInfo.getResource().getURL());
        }

        public ClassResource(String name, URL url) {
            this.id = toClassResourceId(name);
            this.url = url;
        }

        public String getId() {
            return id;
        }

        public URL getUrl() {
            return url;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassResource that = (ClassResource) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(url, that.url);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, url);
        }
    }
}
