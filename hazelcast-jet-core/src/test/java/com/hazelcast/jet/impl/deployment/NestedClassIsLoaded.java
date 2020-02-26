package com.hazelcast.jet.impl.deployment;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NestedClassIsLoaded extends AbstractProcessor {

    @Override
    protected void init(@Nonnull Processor.Context context) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> clazz = cl.loadClass("com.sample.lambda.Worker$InnerWorker");
            Method method = clazz.getMethod("map", String.class);
            assertEquals(method.invoke(clazz.getDeclaredConstructor().newInstance(), "some-string"), "some-string");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
