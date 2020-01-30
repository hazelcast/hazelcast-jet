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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.toList;

public abstract class AbstractJetInstance implements JetInstance {
    private final HazelcastInstance hazelcastInstance;
    private final JetCacheManagerImpl cacheManager;
    private final Supplier<JobRepository> jobRepository;
    private final Map<String, Observable> observables = new ConcurrentHashMap<>();

    public AbstractJetInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.cacheManager = new JetCacheManagerImpl(this);
        this.jobRepository = Util.memoizeConcurrent(() -> new JobRepository(this));
    }

    @Nonnull @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        long jobId = uploadResourcesAndAssignId(config);
        return newJobProxy(jobId, dag, config);
    }

    @Nonnull @Override
    public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
        if (config.getName() == null) {
            return newJob(dag, config);
        } else {
            while (true) {
                Job job = getJob(config.getName());
                if (job != null) {
                    JobStatus status = job.getStatus();
                    if (status != JobStatus.FAILED && status != JobStatus.COMPLETED) {
                        return job;
                    }
                }

                try {
                    return newJob(dag, config);
                } catch (JobAlreadyExistsException e) {
                    logFine(getLogger(), "Could not submit job with duplicate name: %s, ignoring", config.getName());
                }
            }
        }
    }

    @Override
    public Job getJob(long jobId) {
        try {
            Job job = newJobProxy(jobId);
            // get the status for the side-effect of throwing an exception if the jobId is invalid
            job.getStatus();
            return job;
        } catch (Throwable t) {
            if (peel(t) instanceof JobNotFoundException) {
                return null;
            }
            throw rethrow(t);
        }
    }

    @Nonnull @Override
    public List<Job> getJobs(@Nonnull String name) {
        return toList(getJobIdsByName(name), this::newJobProxy);
    }

    @Nonnull @Override
    public Cluster getCluster() {
        return getHazelcastInstance().getCluster();
    }

    @Nonnull @Override
    public String getName() {
        return hazelcastInstance.getName();
    }

    @Nonnull @Override
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    @Nonnull @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return hazelcastInstance.getMap(name);
    }

    @Nonnull @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return hazelcastInstance.getReplicatedMap(name);
    }

    @Nonnull @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return hazelcastInstance.getList(name);
    }

    @Nonnull @Override
    public <T> ITopic<T> getReliableTopic(@Nonnull String name) {
        return hazelcastInstance.getReliableTopic(name);
    }

    @Nonnull @Override
    public JetCacheManager getCacheManager() {
        return cacheManager;
    }

    @Nonnull @Override
    public <T> Observable<T> getObservable(@Nonnull String name) {
        //noinspection unchecked
        return observables.computeIfAbsent(name, observableName ->
                new ObservableImpl<T>(observableName, hazelcastInstance, this::onDestroy, getLogger()));
    }

    @Nonnull
    @Override
    public Collection<Observable<?>> getObservables() {
        return hazelcastInstance.getDistributedObjects().stream()
                  .filter(o -> o.getServiceName().equals(RingbufferService.SERVICE_NAME))
                  .filter(o -> o.getName().startsWith(ObservableImpl.JET_OBSERVABLE_NAME_PREFIX))
                  .map(o -> o.getName().substring(ObservableImpl.JET_OBSERVABLE_NAME_PREFIX.length()))
                  .map(this::getObservable)
                  .collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        observables.values().forEach(Observable::destroy);
        hazelcastInstance.shutdown();
    }

    private void onDestroy(Observable<?> observable) {
        observables.remove(observable.name());
    }

    public abstract boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName);

    private long uploadResourcesAndAssignId(JobConfig config) {
        return jobRepository.get().uploadJobResources(config);
    }

    public abstract ILogger getLogger();
    public abstract Job newJobProxy(long jobId);
    public abstract Job newJobProxy(long jobId, DAG dag, JobConfig config);
    public abstract List<Long> getJobIdsByName(String name);
}
