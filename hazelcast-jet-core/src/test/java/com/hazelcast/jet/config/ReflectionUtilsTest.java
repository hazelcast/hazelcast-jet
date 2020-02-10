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

import com.hazelcast.jet.config.ReflectionUtils.PackageContent;
import org.junit.Test;

import java.net.URL;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;

public class ReflectionUtilsTest {

    @Test
    public void shouldDiscoverAllMemberClasses() throws ClassNotFoundException {
        // When
        Collection<Class<?>> classes = ReflectionUtils.memberClassesOf(OuterClass.class);

        // Then
        assertThat(classes, hasSize(3));
        assertThat(classes, containsInAnyOrder(
                OuterClass.class,
                OuterClass.NestedClass.class,
                Class.forName("com.hazelcast.jet.config.ReflectionUtilsTest$OuterClass$1")
        ));
    }

    @Test
    public void shouldDiscoverAllClassesAndResourcesInAPackage() {
        // When
        PackageContent content = ReflectionUtils.contentOf(OuterClass.class.getPackage().getName());

        // Then
        Collection<Class<?>> classes = content.classes().collect(toList());
        assertThat(classes, hasSize(greaterThan(3)));
        assertThat(classes, hasItem(OuterClass.class));

        List<URL> resources = content.resources().collect(toList());
        assertThat(resources, hasSize(1));
        assertThat(resources, hasItem(hasToString(containsString("package.properties"))));
    }

    @SuppressWarnings("unused")
    private static class OuterClass {
        private void method() {
            new Object() {
            };
        }

        private static class NestedClass {
        }
    }
}
