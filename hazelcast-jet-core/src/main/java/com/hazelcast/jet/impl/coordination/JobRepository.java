package com.hazelcast.jet.impl.coordination;

import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.impl.StartableJob;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.zip.DeflaterOutputStream;

public class JobRepository {

    private static final String RANDOM_IDS_MAP_NAME = "__jet_random_ids";

    private static final String RESOURCES_MAP_NAME = "__jet_job_resources";

    private static final String STARTABLE_JOBS_MAP_NAME = "__jet_startable_jobs";

    // TODO [basri] we should be able to configure backup counts of internal imaps

    private final HazelcastInstance instance;


    public JobRepository(HazelcastInstance instance) {
        this.instance = instance;
    }

    public long generateRandomId() {
        IMap<Long, Long> randomIdsMap = getRandomIdsMap();
        long randomId;
        do {
            randomId = Util.secureRandomNextLong();
        } while (randomIdsMap.putIfAbsent(randomId, randomId) != null);

        return randomId;
    }

    private IMap<Long, Long> getRandomIdsMap() {
        return instance.getMap(RANDOM_IDS_MAP_NAME);
    }

    public StartableJob createStartableJob(long jobId, DAG dag) {
        StartableJob startableJob = new StartableJob(jobId, dag);
        IMap<Long, StartableJob> startableJobsMap = getStartableJobsMap();
        StartableJob prev = startableJobsMap.putIfAbsent(jobId, startableJob);
        if (prev != null) {
            throw new IllegalStateException("Cannot create new startable job with id: " + jobId
                    + " because another job for same id already exists with dag: " + prev.getDag());
        }

        return startableJob;
    }

    private IMap<Long, StartableJob> getStartableJobsMap() {
        return instance.getMap(STARTABLE_JOBS_MAP_NAME);
    }

    public Set<Long> getAllStartableJobIds() {
        IMap<Long, StartableJob> startableJobsMap = getStartableJobsMap();
        return startableJobsMap.keySet();
    }

    public StartableJob getStartableJob(long jobId) {
        IMap<Long, StartableJob> startableJobsMap = getStartableJobsMap();
        return startableJobsMap.get(jobId);
    }

    public long getJobCreationTimeOrFail(long jobId) {
        IMap<Long, StartableJob> startableJobsMap = getStartableJobsMap();
        EntryView<Long, StartableJob> entryView = startableJobsMap.getEntryView(jobId);
        if (entryView != null) {
            return entryView.getCreationTime();
        }

        throw new IllegalArgumentException("Job creation time not found for job id: " + jobId);
    }

    public void uploadJobResources(long jobId, JobConfig jobConfig) {
        IMap<String, byte[]> jobResourcesMap = getJobResourcesMap(jobId);
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
    }

    private IMap<String, byte[]> getJobResourcesMap(long jobId) {
        return instance.getMap(RESOURCES_MAP_NAME + "_" + jobId);
    }

    public void removeStartableJobAndResources(long jobId) {
        IMap<Long, StartableJob> startableJobsMap = getStartableJobsMap();
        startableJobsMap.remove(jobId);

        IMap<String, byte[]> jobResourcesMap = getJobResourcesMap(jobId);
        jobResourcesMap.clear();
        jobResourcesMap.destroy();
    }

    public boolean containsJobResource(long jobId, String resourceId) {
        IMap<String, byte[]> jobResourcesMap = getJobResourcesMap(jobId);
        return jobResourcesMap.containsKey(resourceId);
    }

    public byte[] getJobResource(long jobId, String resourceId) {
        IMap<String, byte[]> jobResourcesMap = getJobResourcesMap(jobId);
        return jobResourcesMap.get(resourceId);
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
