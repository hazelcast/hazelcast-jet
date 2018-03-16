/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.spi.JobClassLoaderFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;

@RunWith(HazelcastSerialClassRunner.class)
public class JobClassLoaderFactoryTest extends JetTestSupport {

    private JetInstance instance;

    @Before
    public void setup() {
        TestJobClassLoderFactory.called = false;
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setClassLoader(new TestClassLoader());
        instance = createJetMember(config);
    }

    @Test
    public void testJobClassLoaderFactoryCall() throws ExecutionException, InterruptedException {
        URL.setURLStreamHandlerFactory(new TestURLStreamHandlerFactory());
        assertFalse(TestJobClassLoderFactory.called);
        DAG dag = new DAG().vertex(new Vertex("test", SourceProcessors.readListP("test")));
        Job job = instance.newJob(dag);
        job.getFuture().get();
        assertTrue(TestJobClassLoderFactory.called);
    }

    public static class TestJobClassLoderFactory implements JobClassLoaderFactory {

        static boolean called;

        @Override
        public ClassLoader getJobClassLoader(long jobId, HazelcastInstance hi, Map<String, byte[]> resources) {
            called = true;
            return getClass().getClassLoader();
        }

    }


    public static class TestURLStreamHandlerFactory implements URLStreamHandlerFactory {

        static final String PROTOCOL = "test";

        @Override
        public URLStreamHandler createURLStreamHandler(String protocol) {
            if (PROTOCOL.equals(protocol)) {
                return new URLStreamHandler() {

                    @Override
                    protected URLConnection openConnection(URL u) throws IOException {
                        return new URLConnection(u) {

                            @Override
                            public void connect() throws IOException {
                            }

                            @Override
                            public InputStream getInputStream() throws IOException {
                                return new ByteArrayInputStream(
                                        "com.hazelcast.jet.core.JobClassLoaderFactoryTest$TestJobClassLoderFactory"
                                        .getBytes());
                            }
                        };
                    }
                };
            }
            return null;
        }
    }

    static class TestClassLoader extends ClassLoader {
        static String ID_NAME = "META-INF/services/com.hazelcast.jet.spi.JobClassLoaderFactory";

        @Override
        public URL findResource(String name) {
            if (ID_NAME.equals(name)) {
                try {
                    return new URL(TestURLStreamHandlerFactory.PROTOCOL + "://test");
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
            }
            return super.findResource(name);
        }

        @Override
        public Enumeration<URL> findResources(String name) throws IOException {
            if (ID_NAME.equals(name)) {
                return Collections.enumeration(Arrays.asList(findResource(name)));
            }
            return super.findResources(name);
        }
    }

}
