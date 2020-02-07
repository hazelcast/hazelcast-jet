package com.hazelcast.jet.config

import org.hamcrest.Matchers
import org.junit.Assert
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
        val resourceConfigs: Collection<ResourceConfig> = config!!.resourceConfigs.values
        Assert.assertThat(resourceConfigs, Matchers.hasSize(4))
        Assert.assertThat(resourceConfigs, Matchers.containsInAnyOrder(
                Matchers.hasProperty("id", Matchers.`is`(OuterClass::class.java.name.replace('.', '/') + ".class")),
                Matchers.hasProperty("id", Matchers.`is`(OuterClass::class.java.name.replace('.', '/') + "\$method\$1.class")),
                Matchers.hasProperty("id", Matchers.`is`(OuterClass::class.java.name.replace('.', '/') + "\$method\$lambda\$1.class")),
                Matchers.hasProperty("id", Matchers.`is`(OuterClass.NestedClass::class.java.name.replace('.', '/') + ".class"))
        ))
    }

    @Suppress("unused", "UNUSED_VARIABLE")
    class OuterClass {
        private fun method() {
            object : Any() {}

            val lambda = { -> }
        }

        class NestedClass
    }
}
