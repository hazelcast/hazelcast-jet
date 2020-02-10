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

package com.hazelcast.jet;

import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class ReflectionsTest {

    @Test
    public void shouldDiscoverAllMemberClasses() throws ClassNotFoundException {
        // When
        Collection<Class<?>> classes = Reflections.memberClassesOf(OuterClass.class);

        // Then
        assertThat(classes, hasSize(3));
        assertThat(classes, containsInAnyOrder(
                OuterClass.class,
                OuterClass.NestedClass.class,
                Class.forName("com.hazelcast.jet.ReflectionsTest$OuterClass$1")
        ));
    }

    @Test
    public void shouldDiscoverAllClassesInAPackage() {
        // When
        Collection<Class<?>> classes = Reflections.memberClassesOf(OuterClass.class.getPackage());

        // Then
        assertThat(classes, hasSize(greaterThan(3)));
        assertThat(classes, hasItem(OuterClass.class));
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
