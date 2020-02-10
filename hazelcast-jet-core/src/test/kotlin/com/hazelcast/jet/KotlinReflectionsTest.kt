package com.hazelcast.jet

import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.hasSize
import org.junit.Assert.assertThat
import org.junit.Test

class KotlinReflectionsTest {

    @Test
    fun shouldAddNestedAndAnonymousClasses() {
        // When
        val classes = Reflections.memberClassesOf(OuterClass::class.java)

        // Then
        assertThat(classes, hasSize(4))
        assertThat(classes, containsInAnyOrder(
                OuterClass::class.java,
                OuterClass.NestedClass::class.java,
                Class.forName("com.hazelcast.jet.KotlinReflectionsTest\$OuterClass\$method\$1"),
                Class.forName("com.hazelcast.jet.KotlinReflectionsTest\$OuterClass\$method\$lambda\$1")
        ))
    }

    @Suppress("unused", "UNUSED_VARIABLE")
    class OuterClass {
        private fun method() {
            object : Any() {}

            val lambda = { }
        }

        class NestedClass
    }
}
