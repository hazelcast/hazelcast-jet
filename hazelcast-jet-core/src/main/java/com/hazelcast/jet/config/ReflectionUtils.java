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

package com.hazelcast.jet.config;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

public final class ReflectionUtils {

    private ReflectionUtils() {
    }

    @Nonnull
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification =
            "False positive on try-with-resources as of JDK11")
    public static Collection<Class<?>> memberClassesOf(Class<?>... classes) {
        String[] packageNames = stream(classes).map(ReflectionUtils::toPackageName).toArray(String[]::new);
        try (ScanResult scanResult = new ClassGraph()
                .whitelistPackages(packageNames)
                .enableClassInfo()
                .ignoreClassVisibility()
                .scan()) {
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

    @Nonnull
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification =
            "False positive on try-with-resources as of JDK11")
    public static Collection<Class<?>> memberClassesOf(String... packages) {
        try (ScanResult scanResult = new ClassGraph()
                .whitelistPackages(packages)
                .enableClassInfo()
                .ignoreClassVisibility()
                .scan()) {
            return scanResult.getAllClasses()
                             .stream()
                             .map(ClassInfo::loadClass)
                             .collect(toList());
        }
    }

    private static String toPackageName(Class<?> clazz) {
        return Optional.ofNullable(clazz.getPackage().getName()).orElse("");
    }
}
