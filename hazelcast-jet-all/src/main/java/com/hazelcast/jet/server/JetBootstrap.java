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

package com.hazelcast.jet.server;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.stream.IStreamCache;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.jet.stream.IStreamMap;

import java.lang.reflect.Method;
import java.util.function.Supplier;
import java.util.jar.JarFile;

/**
 * A Bootstrapper for {@link JetInstance} which returns a bootstrapped Jet client instance.
 * When used in combination with {@code jet-submit.sh}, it runs the JAR file
 * supplied to {@code jet-submit.sh}. The wrapped instance will then add
 * the given JAR file to all jobs submitted through that instance.
 * <p>
 * In the following example, the JAR file containing {@code CustomJetJob}
 * will be submitted automatically in the {@link JetInstance#newJob(DAG)} method call as part of the
 * {@link JobConfig} when run as {@code jet-submit.sh custom-jet-job.jar}.

 * <pre>
 * public class CustomJetJob {
 *   public static void main(String[] args) {
 *     JetInstance jet = JetBootstrap.getInstance();
 *     jet.newJob(getDAG()).execute().get();
 *   }
 * }
 * </pre>
 */
public final class JetBootstrap {

    private static String jar;
    private static final Supplier<JetBootstrap> SUPPLIER = Util.concurrentMemoize(() ->
            new JetBootstrap(Jet.newJetClient()));
    private final JetInstance instance;

    private JetBootstrap(JetInstance instance) {
        this.instance = new InstanceProxy(instance);
    }

    /**
     * Runs the supplied JAR file and sets the static jar file name
     */
    public static void main(String[] args) throws Exception {
        int argLength = 1;
        if (args.length < argLength) {
            System.err.println("Usage:");
            System.err.println("  jet-submit.sh <JAR> ");
            System.exit(1);
        }

        jar = args[0];

        try (JarFile jarFile = new JarFile(jar)) {
            String mainClass = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
            String[] jobArgs = new String[args.length - argLength];
            System.arraycopy(args, argLength, jobArgs, 0, args.length - argLength);

            ClassLoader classLoader = JetBootstrap.class.getClassLoader();
            Class<?> clazz = classLoader.loadClass(mainClass);
            Method main = clazz.getDeclaredMethod("main", String[].class);
            main.invoke(null, new Object[]{jobArgs});
        }
    }

    /**
     * Returns the bootstrapped {@code JetInstance}
     */
    public static JetInstance getInstance() {
        return SUPPLIER.get().instance;
    }

    private static class InstanceProxy implements JetInstance {

        private final JetInstance instance;

        InstanceProxy(JetInstance instance) {
            this.instance = instance;
        }

        @Override
        public String getName() {
            return instance.getName();
        }

        @Override
        public HazelcastInstance getHazelcastInstance() {
            return instance.getHazelcastInstance();
        }

        @Override
        public Cluster getCluster() {
            return instance.getCluster();
        }

        @Override
        public JetConfig getConfig() {
            return instance.getConfig();
        }

        @Override
        public Job newJob(DAG dag) {
            return newJob(dag, new JobConfig());
        }

        @Override
        public Job newJob(DAG dag, JobConfig config) {
            if (jar != null) {
                config.addJar(jar);
            }
            return instance.newJob(dag, config);
        }

        @Override
        public <K, V> IStreamMap<K, V> getMap(String name) {
            return instance.getMap(name);
        }

        @Override
        public <K, V> IStreamCache<K, V> getCache(String name) {
            return instance.getCache(name);
        }

        @Override
        public <E> IStreamList<E> getList(String name) {
            return instance.getList(name);
        }

        @Override
        public void shutdown() {
            instance.shutdown();
        }
    }
}
