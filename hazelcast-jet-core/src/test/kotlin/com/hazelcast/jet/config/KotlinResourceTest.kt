package com.hazelcast.jet.config

import org.hamcrest.Matchers
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.hasProperty
import org.junit.Assert.assertThat
import org.junit.Before
import org.junit.Test

class KotlinResourceTest {

    private var config: JobConfig? = null

    @Before
    fun setUp() {
        config = JobConfig()
    }

    @Test
    fun shouldAddNestedAndAnonymousClasses() {
        // When
        config!!.addClass(OuterClass::class.java)

        // Then
        val resourceConfigs = config!!.resourceConfigs.values
        assertThat(resourceConfigs, Matchers.hasSize(4))
        assertThat(resourceConfigs, Matchers.containsInAnyOrder(
                hasProperty("id", `is`(OuterClass::class.java.name.replace('.', '/') + ".class")),
                hasProperty("id", `is`(OuterClass::class.java.name.replace('.', '/') + "\$method\$1.class")),
                hasProperty("id", `is`(OuterClass::class.java.name.replace('.', '/') + "\$method\$lambda\$1.class")),
                hasProperty("id", `is`(OuterClass.NestedClass::class.java.name.replace('.', '/') + ".class"))
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
