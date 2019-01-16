/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.jet.server.JetCommandLine.JetVersionProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.RunAll;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.LogManager;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;

@Command(
        name = "jet",
        header = "Hazelcast Jet 0.8-SNAPSHOT",
        description = "Utility for interacting with a Hazelcast Jet cluster. Global options are:%n",
        versionProvider = JetVersionProvider.class,
        mixinStandardHelpOptions = true,
        subcommands = {HelpCommand.class}
)
public class JetCommandLine implements Callable<Void> {

    private static final int MAX_STR_LENGTH = 24;
    private static final int WAIT_INTERVAL_MILLIS = 100;

    @Option(names = {"-f", "--config"},
            description = "Path to the client config XML file. " +
                    "If specified addresses and group name options will be ignored. " +
                    "If neither group name, address or config location are specified then the default XML in the " +
                    "config path will be used."
    )
    private File configXml;

    @Option(names = {"-a", "--addresses"},
            split = ",",
            arity = "1..*",
            description = "Comma-separated list of Jet node addresses in the format <hostname>:<port>"
    )
    private List<String> addresses;

    @Option(names = {"-g", "--group"},
            description = "Group name to use when connecting to the cluster. " +
                    "Must be specified together with the <addresses> parameter.",
            defaultValue = "jet"
    )
    private String groupName;

    @Option(names = {"-v", "--verbose"},
            description = {"Show logs from Jet client"}
    )
    private boolean isVerbose;

    public static void main(String[] args) throws Exception {
        PrintStream out = System.out;
        PrintStream err = System.err;

        CommandLine cmd = new CommandLine(new JetCommandLine());

        if (args.length == 0) {
            cmd.usage(out);
        } else {
            List<Object> parsed = cmd.parseWithHandlers(
                    new RunAll().useOut(out).useAnsi(Ansi.AUTO),
                    new DefaultExceptionHandler<List<Object>>().useErr(err).useAnsi(Ansi.AUTO),
                    args
            );
            // only top command was executed
            if (parsed != null && parsed.size() == 1) {
                cmd.usage(out);
            }
        }
    }

    @Override
    public Void call() {
        return null;
    }

    @Command(description = "Submits a job to the cluster",
            mixinStandardHelpOptions = true
    )
    public void submit(
            @Option(names = {"-s", "--snapshot"},
                    paramLabel = "<snapshot name>",
                    description = "Name of initial snapshot to start the job from"
            ) String snapshotName,
            @Option(names = {"-n", "--name"},
                    paramLabel = "<name>",
                    description = "Name of the job"
            ) String name,
            @Parameters(index = "0",
                    paramLabel = "<jar file>",
                    description = "The jar file to submit"
            ) File file,
            @Parameters(index = "1..*",
                    paramLabel = "<arguments>",
                    description = "arguments to pass to the supplied jar file",
                    defaultValue = ""
            ) List<String> params
    ) throws Exception {
        if (!file.exists()) {
            throw new Exception("File " + file + " could not be found.");
        }
        System.out.printf("Submitting JAR '%s' with arguments %s%n", file, params);
        JetBootstrap.executeJar(getClientConfig(), file.getAbsolutePath(), snapshotName, name, params);
    }

    @Command(
            description = "Suspends a running job",
            mixinStandardHelpOptions = true
    )
    public void suspend(
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to suspend"
            ) String name
    ) throws IOException {
        runWithJet(jet -> {
            Job job = getJob(name, jet);
            if (job.getStatus() != JobStatus.RUNNING) {
                throw new RuntimeException("Job '" + name + "' is not running. Current state: " + job.getStatus());
            }
            System.out.printf("Suspending job %s...%n", formatJob(job));
            job.suspend();
            waitForJobStatus(job, JobStatus.SUSPENDED);
            System.out.println("Job was successfully suspended.");
        });
    }

    @Command(
            description = "Cancels a running job"
    )
    public void cancel(
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to terminate"
            ) String name
    ) throws IOException {
        runWithJet(jet -> {
            Job job = getJob(name, jet);
            assertJobActive(name, job);
            System.out.printf("Terminating job %s...%n", formatJob(job));
            job.cancel();
            waitForJobStatus(job, JobStatus.FAILED);
            System.out.println("Job was successfully terminated.");
        });
    }

    @Command(
            name = "save-snapshot",
            description = "Saves a named snapshot from a job"
    )
    public void saveSnapshot(
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to terminate")
                    String jobName,
            @Parameters(index = "1",
                    paramLabel = "<snapshot name>",
                    description = "Name of the snapshot")
                    String snapshotName,
            @Option(names = {"-C", "--cancel"},
                    description = "Cancel the job after taking the snapshot")
                    boolean isTerminal
    ) throws IOException {
        runWithJet(jet -> {
            Job job = getJob(jobName, jet);
            assertJobActive(jobName, job);
            if (isTerminal) {
                System.out.printf("Saving snapshot with name '%s' from job '%s' and terminating the job...%n",
                        snapshotName, formatJob(job)
                );
                job.cancelAndExportSnapshot(snapshotName);
                waitForJobStatus(job, JobStatus.FAILED);
            } else {
                System.out.printf("Saving snapshot with name '%s' from job '%s'...%n", snapshotName, formatJob(job));
                job.exportSnapshot(snapshotName);
            }
            System.out.println("Snapshot was successfully exported.");
        });
    }

    @Command(
            name = "delete-snapshot",
            description = "Deletes a named snapshot"
    )
    public void deleteSnapshot(
            @Parameters(index = "0",
                    paramLabel = "<snapshot name>",
                    description = "Name of the snapshot")
                    String snapshotName
    ) throws IOException {
        runWithJet(jet -> {
            JobStateSnapshot jobStateSnapshot = jet.getJobStateSnapshot(snapshotName);
            if (jobStateSnapshot == null) {
                throw new JetException(String.format("No snapshot with name %s was found", snapshotName));
            }
            jobStateSnapshot.destroy();
            System.out.println("Snapshot was successfully deleted.");
        });
    }

    @Command(
            description = "Restarts a running job"
    )
    public void restart(
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to restart")
                    String name
    ) throws IOException {
        runWithJet(jet -> {
            Job job = getJob(name, jet);
            assertJobActive(name, job);
            System.out.println("Restarting job " + formatJob(job) + "...");
            job.restart();
            waitForJobStatus(job, JobStatus.RUNNING);
            System.out.println("Job was successfully restarted.");
        });
    }

    @Command(
            description = "Resumes a running job"
    )
    public void resume(
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to resume")
                    String name
    ) throws IOException {
        runWithJet(jet -> {
            Job job = getJob(name, jet);
            if (job.getStatus() != JobStatus.SUSPENDED) {
                throw new RuntimeException("Job '" + name + "' is not suspended. Current state: " + job.getStatus());
            }
            System.out.println("Resuming job " + formatJob(job) + "...");
            job.resume();
            waitForJobStatus(job, JobStatus.RUNNING);
            System.out.println("Job was successfully resumed.");
        });
    }

    @Command(
            description = "Lists running jobs on the cluster"
    )
    public void jobs(
            @Option(names = {"-a", "--all"},
                    description = "Lists all jobs including completed or failed ones")
                    boolean listAll
    ) throws IOException {
        runWithJet(jet -> {
            JetClientInstanceImpl client = (JetClientInstanceImpl) jet;
            List<JobSummary> summaries = client.getJobSummaryList();
            System.out.printf("%-24s %-19s %-18s %-23s%n", "NAME", "ID", "STATUS", "SUBMISSION TIME");

            summaries.stream()
                    .filter(job -> listAll || isActive(job.getStatus()))
                    .forEach(job -> {
                        String idString = idToString(job.getJobId());
                        String name = job.getName().equals(idString) ? "N/A" : shorten(job.getName(), MAX_STR_LENGTH);
                        LocalDateTime submissionTime = toLocalDateTime(job.getSubmissionTime());
                        System.out.printf("%-24s %-19s %-18s %-23s%n", name, idString, job.getStatus(), submissionTime);
                    });
        });
    }

    @Command(
            description = "Lists saved snapshots on the cluster"
    )
    public void snapshots(
    ) throws IOException {
        runWithJet(jet -> {
            Collection<JobStateSnapshot> snapshots = jet.getJobStateSnapshots();
            System.out.printf("%-24s %-15s %-23s %-24s%n", "NAME", "SIZE (bytes)", "TIME", "JOB NAME");
            snapshots.stream()
                    .forEach(ss -> {
                        String jobName = ss.jobName() == null ? Util.idToString(ss.jobId()) : ss.jobName();
                        jobName = shorten(jobName, MAX_STR_LENGTH);
                        String ssName = shorten(ss.name(), MAX_STR_LENGTH);
                        LocalDateTime creationTime = toLocalDateTime(ss.creationTime());
                        System.out.printf("%-24s %-,15d %-23s %-24s%n", ssName, ss.payloadSize(), creationTime, jobName);
                    });
        });
    }

    private void runWithJet(Consumer<JetInstance> consumer) throws IOException {
        configureLogging();
        JetInstance jet = Jet.newJetClient(getClientConfig());
        try {
            consumer.accept(jet);
        } finally {
            jet.shutdown();
        }
    }

    private ClientConfig getClientConfig() throws IOException {
        if (configXml != null) {
            return new XmlClientConfigBuilder(configXml).build();
        }
        if (addresses != null) {
            ClientConfig config = new ClientConfig();
            config.getNetworkConfig().addAddress(addresses.toArray(new String[0]));
            config.getGroupConfig().setName(groupName);
            return config;
        }
        return XmlJetConfigBuilder.getClientConfig();
    }

    private void configureLogging() throws IOException {
       StartServer.configureLogging();
       LogManager.getLogManager().getLogger("").setLevel(isVerbose ? Level.INFO : Level.WARNING);
    }

    private static Job getJob(String nameOrId, JetInstance jet) {
        Job job = jet.getJob(nameOrId);
        if (job == null) {
            job = jet.getJob(Util.idFromString(nameOrId));
            if (job == null) {
                throw new RuntimeException("No job with name or id '" + nameOrId + "' was found.");
            }
        }
        return job;
    }

    private static String shorten(String name, int length) {
        return name.substring(0, Math.min(name.length(), length));
    }

    private static String formatJob(Job job) {
        return "id=" + idToString(job.getId())
                + ", name=" + job.getName()
                + ", submissionTime=" + toLocalDateTime(job.getSubmissionTime());
    }

    private static void assertJobActive(String name, Job job) {
        if (!isActive(job.getStatus())) {
            throw new RuntimeException("Job '" + name + "' is not active. Current state: " + job.getStatus());
        }
    }

    private static void waitForJobStatus(Job job, JobStatus status) {
        while (job.getStatus() != status) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(WAIT_INTERVAL_MILLIS));
        }
    }

    private static boolean isActive(JobStatus status) {
        return status != JobStatus.FAILED && status != JobStatus.COMPLETED;
    }

    public static class JetVersionProvider implements IVersionProvider {

        @Override
        public String[] getVersion() {
            JetBuildInfo jetBuildInfo = BuildInfoProvider.getBuildInfo().getJetBuildInfo();
            return new String[]{
                    "Hazelcast Jet " + jetBuildInfo.getVersion(),
                    "Revision " + jetBuildInfo.getRevision(),
                    "Build " + jetBuildInfo.getBuild()
            };
        }
    }
}
