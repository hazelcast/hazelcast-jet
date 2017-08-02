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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobResourceKey;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.zip.DeflaterOutputStream;

import static com.hazelcast.jet.config.JetConfig.IDS_MAP_NAME;
import static com.hazelcast.jet.config.JetConfig.JOB_RECORDS_MAP_NAME;
import static com.hazelcast.jet.config.JetConfig.RESOURCES_MAP_NAME_PREFIX;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static java.util.concurrent.TimeUnit.HOURS;

public class JobRepository {

    static final String RESOURCE_MARKER = "__jet.jobId";
    private static final long JOB_EXPIRATION_DURATION_IN_MILLIS = HOURS.toMillis(2);

    // TODO [basri] there is no cleanup for ids yet

    private final HazelcastInstance instance;
    private final IMap<Long, Long> jobIds;
    private final IMap<Long, JobRecord> jobs;
    private long jobExpirationDurationInMillis = JOB_EXPIRATION_DURATION_IN_MILLIS;

    public JobRepository(JetInstance jetInstance) {
        this.instance = jetInstance.getHazelcastInstance();
        this.jobIds = instance.getMap(IDS_MAP_NAME);
        this.jobs = instance.getMap(JOB_RECORDS_MAP_NAME);
    }

    void setJobExpirationDurationInMillis(long jobExpirationDurationInMillis) {
        this.jobExpirationDurationInMillis = jobExpirationDurationInMillis;
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

    void putNewJobRecord(JobRecord jobRecord) {
        long jobId = jobRecord.getJobId();
        JobRecord prev = jobs.putIfAbsent(jobId, jobRecord);
        if (prev != null && !prev.getDag().equals(jobRecord.getDag())) {
            throw new IllegalStateException("Cannot put job record for job " + idToString(jobId)
                    + " because it already exists with a different dag");
        }
    }

    Collection<JobRecord> getJobRecords() {
        return jobs.values();
    }

    public JobRecord getJob(long jobId) {
       return jobs.get(jobId);
    }

    <T> IMap<JobResourceKey, T> getJobResources(long jobId) {
        return instance.getMap(RESOURCES_MAP_NAME_PREFIX + jobId);
    }

    public void uploadJobResources(long jobId, JobConfig jobConfig) {
        IMap<JobResourceKey, Object> jobResourcesMap = getJobResources(jobId);
        for (ResourceConfig rc : jobConfig.getResourceConfigs()) {
            Map<JobResourceKey, byte[]> tmpMap = new HashMap<>();
            if (rc.isArchive()) {
                try {
                    loadJar(jobId, tmpMap, rc.getUrl());
                } catch (IOException e) {
                    // TODO basri: fix it
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    InputStream in = rc.getUrl().openStream();
                    readStreamAndPutCompressedToMap(new JobResourceKey(jobId, rc.getId()), tmpMap, in);
                } catch (IOException e) {
                    // TODO basri: fix it
                    throw new RuntimeException(e);
                }
            }

            // now upload it all
            jobResourcesMap.putAll(tmpMap);
        }

        jobResourcesMap.put(new JobResourceKey(jobId, RESOURCE_MARKER), jobId);
    }

    boolean isResourceUploadCompleted(long jobId) {
        Object val = getJobResources(jobId).get(new JobResourceKey(jobId, RESOURCE_MARKER));
        return (val instanceof Long && jobId == ((Long) val));
    }

    /**
     * Perform cleanup after job completion
     */
    void deleteJob(long jobId) {
        jobs.remove(jobId);
        IMap<JobResourceKey, Object> jobResourcesMap = getJobResources(jobId);
        if (jobResourcesMap != null) {
            jobResourcesMap.clear();
            jobResourcesMap.destroy();
        }
    }

    long getJobCreationTime(long jobId) throws IllegalArgumentException {
        EntryView<Long, JobRecord> entryView = jobs.getEntryView(jobId);
        if (entryView != null) {
            return entryView.getCreationTime();
        }
        throw new IllegalArgumentException("Job creation time not found for job id: " + idToString(jobId));
    }

    void cleanup(Set<Long> completedJobIds, Set<Long> runningJobIds) {
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
                    IMap<JobResourceKey, Object> resources = getJobResources(jobId);
                    JobResourceKey markerKey = new JobResourceKey(jobId, RESOURCE_MARKER);
                    EntryView<JobResourceKey, Object> marker = resources.getEntryView(markerKey);
                    if (marker == null) {
                        resources.putIfAbsent(markerKey, RESOURCE_MARKER);
                    } else if (isJobExpired(marker.getCreationTime())) {
                        deleteJob(jobId);
                    }
                });
    }

    private boolean isResourcesMap(DistributedObject obj) {
        return MapService.SERVICE_NAME.equals(obj.getServiceName())
                && obj.getName().startsWith(RESOURCES_MAP_NAME_PREFIX);
    }

    private long getJobIdFromResourcesMapName(String mapName) {
        String s = mapName.substring(RESOURCES_MAP_NAME_PREFIX.length());
        return Long.parseLong(s);
    }

    private boolean isJobExpired(long creationTime) {
        return (System.currentTimeMillis() - creationTime) >= jobExpirationDurationInMillis;
    }

    /**
     * Unzips the Jar archive and processes individual entries using
     * {@link #readStreamAndPutCompressedToMap(JobResourceKey, Map, InputStream)}.
     */
    private void loadJar(long jobId, Map<JobResourceKey, byte[]> map, URL url) throws IOException {
        try (JarInputStream jis = new JarInputStream(new BufferedInputStream(url.openStream()))) {
            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }
                readStreamAndPutCompressedToMap(new JobResourceKey(jobId, jarEntry.getName()), map, jis);
            }
        }
    }

    private void readStreamAndPutCompressedToMap(JobResourceKey resourceKey,
                                                 Map<JobResourceKey, byte[]> map, InputStream in)
            throws IOException {
        // ignore duplicates: the first resource in first jar takes precedence
        if (map.containsKey(resourceKey)) {
            return;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
            IOUtil.drainTo(in, compressor);
        }

        map.put(resourceKey, baos.toByteArray());
    }

}
