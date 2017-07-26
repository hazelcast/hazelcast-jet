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

package com.hazelcast.jet.impl.coordination;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.zip.DeflaterOutputStream;

public class JobRepository {

    private static final String IDS_MAP_NAME = "__jet.jobs.ids";
    private static final String RESOURCES_MAP_NAME_PREFIX = "__jet.jobs.resources.";
    private static final String RESOURCE_MARKER = "__jet.jobId";
    private static final String JOB_RECORDS_MAP_NAME = "__jet.jobs.records";
    private static final long RESOURCE_MAP_EXPIRATION_TIME_IN_MILLIS = TimeUnit.HOURS.toMillis(1);

    // TODO [basri] we should be able to configure backup counts of internal imaps

    private final HazelcastInstance instance;
    private final IMap<Long, Long> jobIds;
    private final IMap<Long, JobRecord> jobs;

    public JobRepository(HazelcastInstance instance) {
        this.instance = instance;
        this.jobIds = instance.getMap(IDS_MAP_NAME);
        this.jobs = instance.getMap(JOB_RECORDS_MAP_NAME);
    }

    /**
     * Generate a new ID, guaranteed to be unique across the cluster
     */
    public long newId() {
        long id;
        do {
            id = Util.secureRandomNextLong();
        } while (jobIds.putIfAbsent(id, id) != null);
        return id;
    }

    public JobRecord newJobRecord(long jobId, DAG dag) {
        JobRecord jobRecord = new JobRecord(jobId, dag);
        IMap<Long, JobRecord> jobRecords = getJobs();
        JobRecord prev = jobRecords.putIfAbsent(jobId, jobRecord);
        if (prev != null) {
            throw new IllegalStateException("Cannot create new job record with id: " + jobId
                    + " because another job for same id already exists with dag: " + prev.getDag());
        }

        return jobRecord;
    }

    public IMap<Long, JobRecord> getJobs() {
        return jobs;
    }

    public JobRecord getJob(long jobId) {
       return jobs.get(jobId);
    }

    public <T> IMap<String, T> getJobResources(long jobId) {
        return instance.getMap(RESOURCES_MAP_NAME_PREFIX + jobId);
    }

    /**
     * Perform cleanup after job completion
     */
    public void deleteJob(long jobId) {
        jobs.remove(jobId);
        IMap<String, Object> jobResourcesMap = getJobResources(jobId);
        if (jobResourcesMap != null) {
            jobResourcesMap.clear();
            jobResourcesMap.destroy();
        }
    }

    public long getJobCreationTime(long jobId) throws IllegalArgumentException {
        EntryView<Long, JobRecord> entryView = jobs.getEntryView(jobId);
        if (entryView != null) {
            return entryView.getCreationTime();
        }
        throw new IllegalArgumentException("Job creation time not found for job id: " + jobId);
    }

    public void uploadJobResources(long jobId, JobConfig jobConfig) {
        IMap<String, Object> jobResourcesMap = getJobResources(jobId);
        for (ResourceConfig rc : jobConfig.getResourceConfigs()) {
            Map<String, byte[]> tmpMap = new HashMap<>();
            if (rc.isArchive()) {
                try {
                    loadJar(tmpMap, rc.getUrl());
                } catch (IOException e) {
                    // TODO basri: fix it
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    readStreamAndPutCompressedToMap(tmpMap, rc.getUrl().openStream(), rc.getId());
                } catch (IOException e) {
                    // TODO basri: fix it
                    throw new RuntimeException(e);
                }
            }

            // now upload it all
            jobResourcesMap.putAll(tmpMap);
        }

        jobResourcesMap.put(RESOURCE_MARKER, RESOURCE_MARKER);
    }

    public void cleanup(Set<Long> completedJobIds, Set<Long> runningJobIds) {
        // clean up completed jobs
        completedJobIds.forEach(this::deleteJob);

        // clean up expired jobs which are not still running
        jobs.keySet()
            .stream()
            .filter(jobId -> !runningJobIds.contains(jobId))
            .filter(jobId -> {
                EntryView<Long, JobRecord> view = jobs.getEntryView(jobId);
                return view != null && isJobExpired(view.getCreationTime());
            })
            .forEach(this::deleteJob);

        // clean up expired resources
        instance.getDistributedObjects()
                .stream()
                .filter(this::isResourcesMap)
                .map(DistributedObject::getName)
                .map(this::getJobIdFromResourcesMapName)
                .filter(jobId -> !runningJobIds.contains(jobId))
                .forEach(jobId -> {
                    IMap<String, Object> resources = getJobResources(jobId);
                    EntryView<String, Object> marker = resources.getEntryView(RESOURCE_MARKER);
                    if (marker == null) {
                        resources.putIfAbsent(RESOURCE_MARKER, RESOURCE_MARKER);
                    } else if (isJobExpired(marker.getCreationTime())) {
                        deleteJob(jobId);
                    }
                });
    }

    private boolean isResourcesMap(DistributedObject obj) {
        return MapService.SERVICE_NAME.equals(obj.getServiceName()) && obj.getName().startsWith(RESOURCES_MAP_NAME_PREFIX);
    }

    private long getJobIdFromResourcesMapName(String mapName) {
        String s = mapName.substring(RESOURCES_MAP_NAME_PREFIX.length());
        return Long.parseLong(s);
    }

    private boolean isJobExpired(long creationTime) {
        return (System.currentTimeMillis() - creationTime) >= RESOURCE_MAP_EXPIRATION_TIME_IN_MILLIS;
    }

    /**
     * Unzips the Jar archive and processes individual entries using
     * {@link #readStreamAndPutCompressedToMap(Map, InputStream, String)}.
     */
    private void loadJar(Map<String, byte[]> map, URL url) throws IOException {
        try (JarInputStream jis = new JarInputStream(new BufferedInputStream(url.openStream()))) {
            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }
                readStreamAndPutCompressedToMap(map, jis, jarEntry.getName());
            }
        }
    }

    private void readStreamAndPutCompressedToMap(Map<String, byte[]> map, InputStream in, String resourceId)
            throws IOException {
        // ignore duplicates: the first resource in first jar takes precedence
        if (map.containsKey(resourceId)) {
            return;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
            IOUtil.drainTo(in, compressor);
        }

        map.put(resourceId, baos.toByteArray());
    }

}
