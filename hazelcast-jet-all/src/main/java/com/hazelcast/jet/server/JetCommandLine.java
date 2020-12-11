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

package com.hazelcast.jet.server;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.management.MCClusterMetadata;
import com.hazelcast.client.impl.protocol.codec.MCGetClusterMetadataCodec;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetBootstrap;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.impl.config.ConfigProvider;
import com.hazelcast.jet.server.JetCommandLine.JetVersionProvider;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import org.jline.reader.EOFError;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.History;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.UserInterruptException;
import org.jline.reader.EndOfFileException;

import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunAll;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.IOError;

import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.LogManager;

import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;

@Command(
        name = "jet",
        description = "Utility to perform operations on a Hazelcast Jet cluster.%n" +
                "By default it uses the file config/hazelcast-client.yaml to configure the client connection." +
                "%n%n" +
                "Global options are:%n",
        versionProvider = JetVersionProvider.class,
        mixinStandardHelpOptions = true,
        sortOptions = false,
        subcommands = {HelpCommand.class}
)
public class JetCommandLine implements Runnable {

    private static final int MAX_STR_LENGTH = 24;
    private static final int WAIT_INTERVAL_MILLIS = 100;
    private static final int SQL_LIST_MAX = 100;
    private static final int PRIMARY_COLOR = AttributedStyle.YELLOW;
    private static final int SECONDARY_COLOR = 12;

    private final Function<ClientConfig, JetInstance> jetClientFn;
    private final PrintStream out;
    private final PrintStream err;

    @Option(names = {"-f", "--config"},
            description = "Optional path to a client config XML/YAML file." +
                    " The default is to use config/hazelcast-client.yaml.",
            order = 0
    )
    private File config;

    @Option(names = {"-a", "--addresses"},
            split = ",",
            arity = "1..*",
            paramLabel = "<hostname>:<port>",
            description = "[DEPRECATED] Optional comma-separated list of Jet node addresses in the format" +
                    " <hostname>:<port>, if you want to connect to a cluster other than the" +
                    " one configured in the configuration file. Use --targets instead.",
            order = 1
    )
    private List<String> addresses;

    @Option(names = {"-n", "--cluster-name"},
        description = "[DEPRECATED] The cluster name to use when connecting to the cluster " +
            "specified by the <addresses> parameter. Use --targets instead.",
        defaultValue = "jet",
        showDefaultValue = Visibility.ALWAYS,
        order = 2
    )
    private String clusterName;

    @Mixin(name = "targets")
    private TargetsMixin targetsMixin;

    @Mixin(name = "verbosity")
    private Verbosity verbosity;

    public JetCommandLine(Function<ClientConfig, JetInstance> jetClientFn, PrintStream out, PrintStream err) {
        this.jetClientFn = jetClientFn;
        this.out = out;
        this.err = err;
    }

    public static void main(String[] args) {
        runCommandLine(Jet::newJetClient, System.out, System.err, true, args);
    }

    static void runCommandLine(
            Function<ClientConfig, JetInstance> jetClientFn,
            PrintStream out, PrintStream err,
            boolean shouldExit,
            String[] args
    ) {
        CommandLine cmd = new CommandLine(new JetCommandLine(jetClientFn, out, err));
        cmd.getSubcommands().get("submit").setStopAtPositional(true);

        String jetVersion = getBuildInfo().getJetBuildInfo().getVersion();
        cmd.getCommandSpec().usageMessage().header("Hazelcast Jet " + jetVersion);

        if (args.length == 0) {
            cmd.usage(out);
        } else {
            DefaultExceptionHandler<List<Object>> excHandler =
                    new ExceptionHandler<List<Object>>().useErr(err).useAnsi(Ansi.AUTO);
            if (shouldExit) {
                excHandler.andExit(1);
            }
            List<Object> parsed = cmd.parseWithHandlers(new RunAll().useOut(out).useAnsi(Ansi.AUTO), excHandler, args);
            // only top command was executed
            if (parsed != null && parsed.size() == 1) {
                cmd.usage(out);
            }
        }
    }

    @Override
    public void run() {
        // top-level command, do nothing
    }

    @Command(description = "Starts the SQL shell [BETA]")
    public void sql(@Mixin(name = "verbosity") Verbosity verbosity,
                    @Mixin(name = "targets") TargetsMixin targets
    ) {
        runWithJet(targets, verbosity, jet -> {
            LineReader reader = LineReaderBuilder.builder().parser(new MultilineParser())
                    .variable(LineReader.SECONDARY_PROMPT_PATTERN, new AttributedStringBuilder()
                            .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR)).append("%M%P > ").toAnsi())
                    .variable(LineReader.INDENTATION, 2)
                    .variable(LineReader.LIST_MAX, SQL_LIST_MAX)
                    .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
                    .appName("hazelcast-jet-sql")
                    .build();
            PrintWriter writer = reader.getTerminal().writer();
            writer.println(CliPrompts.STARTING_PROMPT);
            writer.flush();

            for (; ; ) {
                String command;
                try {
                    command = reader.readLine(new AttributedStringBuilder()
                            .style(AttributedStyle.DEFAULT.foreground(SECONDARY_COLOR))
                            .append("JET SQL> ").toAnsi()).trim();
                } catch (UserInterruptException | EndOfFileException | IOError e) {
                    // Ctrl+C, Ctrl+D, and kill signals result in exit
                    writer.println(CliPrompts.EXIT_PROMPT);
                    writer.flush();
                    break;
                }

                if (command.lastIndexOf(";") >= 0) {
                    command = command.substring(0, command.lastIndexOf(";")).trim();
                }

                if ("".equals(command)) {
                    continue;
                }
                if ("clear".equalsIgnoreCase(command)) {
                    reader.getTerminal().puts(InfoCmp.Capability.clear_screen);
                    continue;
                }
                if ("help".equalsIgnoreCase(command)) {
                    writer.println(CliPrompts.HELP_PROMPT);
                    writer.flush();
                    continue;
                }
                if ("history".equalsIgnoreCase(command)) {
                    ListIterator<History.Entry> iterator = reader.getHistory().iterator();
                    while (iterator.hasNext()) {
                        History.Entry entry = iterator.next();
                        if (iterator.hasNext()) {
                            String entryLine = new AttributedStringBuilder()
                                    .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                                    .append(String.valueOf(entry.index() + 1))
                                    .append(" - ")
                                    .append(entry.line())
                                    .append("\n")
                                    .toAnsi();
                            writer.println(entryLine);
                            writer.flush();
                        } else {
                            iterator.remove();
                        }
                    }
                    continue;
                }
                if ("exit".equalsIgnoreCase(command)) {
                    writer.println(CliPrompts.EXIT_PROMPT);
                    writer.flush();
                    break;
                }

                executeSqlCmd(jet, command, reader.getTerminal());
            }
        });
    }

    @Command(description = "Submits a job to the cluster")
    public void submit(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Option(names = {"-s", "--snapshot"},
                    paramLabel = "<snapshot name>",
                    description = "Name of the initial snapshot to start the job from"
            ) String snapshotName,
            @Option(names = {"-n", "--name"},
                    paramLabel = "<name>",
                    description = "Name of the job"
            ) String name,
            @Option(names = {"-c", "--class"},
                    paramLabel = "<class>",
                    description = "Fully qualified name of the main class inside the JAR file"
            ) String mainClass,
            @Parameters(index = "0",
                    paramLabel = "<jar file>",
                    description = "The jar file to submit"
            ) File file,
            @Parameters(index = "1..*",
                    paramLabel = "<arguments>",
                    description = "Arguments to pass to the supplied jar file"
            ) List<String> params
    ) throws Exception {
        if (params == null) {
            params = emptyList();
        }
        this.verbosity.merge(verbosity);
        configureLogging();
        if (!file.exists()) {
            throw new Exception("File " + file + " could not be found.");
        }
        printf("Submitting JAR '%s' with arguments %s", file, params);
        if (name != null) {
            printf("Using job name '%s'", name);
        }
        if (snapshotName != null) {
            printf("Will restore the job from the snapshot with name '%s'", snapshotName);
        }

        targetsMixin.replace(targets);

        JetBootstrap.executeJar(this::getJetClient, file.getAbsolutePath(), snapshotName, name, mainClass, params);
    }

    @Command(description = "Suspends a running job")
    public void suspend(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to suspend"
            ) String name
    ) throws IOException {
        runWithJet(targets, verbosity, jet -> {
            Job job = getJob(jet, name);
            assertJobRunning(name, job);
            printf("Suspending job %s...", formatJob(job));
            job.suspend();
            waitForJobStatus(job, JobStatus.SUSPENDED);
            println("Job suspended.");
        });
    }

    @Command(
            description = "Cancels a running job"
    )
    public void cancel(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to cancel"
            ) String name
    ) throws IOException {
        runWithJet(targets, verbosity, jet -> {
            Job job = getJob(jet, name);
            assertJobActive(name, job);
            printf("Cancelling job %s", formatJob(job));
            job.cancel();
            waitForJobStatus(job, JobStatus.FAILED);
            println("Job cancelled.");
        });
    }

    @Command(
            name = "save-snapshot",
            description = "Exports a named snapshot from a job and optionally cancels it"
    )
    public void saveSnapshot(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to take the snapshot from")
                    String jobName,
            @Parameters(index = "1",
                    paramLabel = "<snapshot name>",
                    description = "Name of the snapshot")
                    String snapshotName,
            @Option(names = {"-C", "--cancel"},
                    description = "Cancel the job after taking the snapshot")
                    boolean isTerminal
    ) throws IOException {
        runWithJet(targets, verbosity, jet -> {
            Job job = getJob(jet, jobName);
            assertJobActive(jobName, job);
            if (isTerminal) {
                printf("Saving snapshot with name '%s' from job '%s' and cancelling the job...",
                        snapshotName, formatJob(job)
                );
                job.cancelAndExportSnapshot(snapshotName);
                waitForJobStatus(job, JobStatus.FAILED);
            } else {
                printf("Saving snapshot with name '%s' from job '%s'...", snapshotName, formatJob(job));
                job.exportSnapshot(snapshotName);
            }
            printf("Exported snapshot '%s'.", snapshotName);
        });
    }

    @Command(
            name = "delete-snapshot",
            description = "Deletes a named snapshot"
    )
    public void deleteSnapshot(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<snapshot name>",
                    description = "Name of the snapshot")
                    String snapshotName
    ) throws IOException {
        runWithJet(targets, verbosity, jet -> {
            JobStateSnapshot jobStateSnapshot = jet.getJobStateSnapshot(snapshotName);
            if (jobStateSnapshot == null) {
                throw new JetException(String.format("Didn't find a snapshot named '%s'", snapshotName));
            }
            jobStateSnapshot.destroy();
            printf("Deleted snapshot '%s'.", snapshotName);
        });
    }

    @Command(
            description = "Restarts a running job"
    )
    public void restart(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to restart")
                    String name
    ) throws IOException {
        runWithJet(targets, verbosity, jet -> {
            Job job = getJob(jet, name);
            assertJobRunning(name, job);
            println("Restarting job " + formatJob(job) + "...");
            job.restart();
            waitForJobStatus(job, JobStatus.RUNNING);
            println("Job restarted.");
        });
    }

    @Command(
            description = "Resumes a suspended job"
    )
    public void resume(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to resume")
                    String name
    ) throws IOException {
        runWithJet(targets, verbosity, jet -> {
            Job job = getJob(jet, name);
            if (job.getStatus() != JobStatus.SUSPENDED) {
                throw new RuntimeException("Job '" + name + "' is not suspended. Current state: " + job.getStatus());
            }
            println("Resuming job " + formatJob(job) + "...");
            job.resume();
            waitForJobStatus(job, JobStatus.RUNNING);
            println("Job resumed.");
        });
    }

    @Command(
            name = "list-jobs",
            description = "Lists running jobs on the cluster"
    )
    public void listJobs(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Option(names = {"-a", "--all"},
                    description = "Lists all jobs including completed and failed ones")
                    boolean listAll
    ) throws IOException {
        runWithJet(targets, verbosity, jet -> {
            JetClientInstanceImpl client = (JetClientInstanceImpl) jet;
            List<JobSummary> summaries = client.getJobSummaryList();
            String format = "%-19s %-18s %-23s %s";
            printf(format, "ID", "STATUS", "SUBMISSION TIME", "NAME");
            summaries.stream()
                     .filter(job -> listAll || isActive(job.getStatus()))
                     .forEach(job -> {
                         String idString = idToString(job.getJobId());
                         String name = job.getName().equals(idString) ? "N/A" : job.getName();
                         printf(format, idString, job.getStatus(), toLocalDateTime(job.getSubmissionTime()), name);
                     });
        });
    }

    @Command(
            name = "list-snapshots",
            description = "Lists exported snapshots on the cluster"
    )
    public void listSnapshots(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets,
            @Option(names = {"-F", "--full-job-name"},
                    description = "Don't trim job name to fit, can break layout")
                    boolean fullJobName) throws IOException {
        runWithJet(targets, verbosity, jet -> {
            Collection<JobStateSnapshot> snapshots = jet.getJobStateSnapshots();
            printf("%-23s %-15s %-24s %s", "TIME", "SIZE (bytes)", "JOB NAME", "SNAPSHOT NAME");
            snapshots.stream()
                     .sorted(Comparator.comparing(JobStateSnapshot::name))
                     .forEach(ss -> {
                         LocalDateTime creationTime = toLocalDateTime(ss.creationTime());
                         String jobName = ss.jobName() == null ? Util.idToString(ss.jobId()) : ss.jobName();
                         if (!fullJobName) {
                             jobName = shorten(jobName);
                         }
                         printf("%-23s %-,15d %-24s %s", creationTime, ss.payloadSize(), jobName, ss.name());
                     });
        });
    }

    @Command(
            description = "Shows current cluster state and information about members"
    )
    public void cluster(
            @Mixin(name = "verbosity") Verbosity verbosity,
            @Mixin(name = "targets") TargetsMixin targets
    ) {
        runWithJet(targets, verbosity, jet -> {
            JetClientInstanceImpl client = (JetClientInstanceImpl) jet;
            HazelcastClientInstanceImpl hazelcastClient = client.getHazelcastClient();
            ClientClusterService clientClusterService = hazelcastClient.getClientClusterService();
            MCClusterMetadata clusterMetadata =
                    FutureUtil.getValue(getClusterMetadata(hazelcastClient, clientClusterService.getMasterMember()));

            Cluster cluster = client.getCluster();

            println("State: " + clusterMetadata.getCurrentState());
            println("Version: " + clusterMetadata.getJetVersion());
            println("Size: " + cluster.getMembers().size());

            println("");

            String format = "%-24s %-19s";
            printf(format, "ADDRESS", "UUID");
            cluster.getMembers().forEach(member -> printf(format, member.getAddress(), member.getUuid()));
        });
    }

    private CompletableFuture<MCClusterMetadata> getClusterMetadata(HazelcastClientInstanceImpl client, Member member) {
        checkNotNull(member);

        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetClusterMetadataCodec.encodeRequest(),
                null,
                member.getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                client.getSerializationService(),
                clientMessage -> {
                    MCGetClusterMetadataCodec.ResponseParameters response =
                            MCGetClusterMetadataCodec.decodeResponse(clientMessage);

                    MCClusterMetadata metadata = new MCClusterMetadata();
                    metadata.setCurrentState(ClusterState.getById(response.currentState));
                    metadata.setClusterTime(response.clusterTime);
                    metadata.setMemberVersion(response.memberVersion);
                    metadata.setJetVersion(response.jetVersion);
                    return metadata;
                }
        );
    }

    private void runWithJet(TargetsMixin targets, Verbosity verbosity, ConsumerEx<JetInstance> consumer) {
        this.targetsMixin.replace(targets);
        this.verbosity.merge(verbosity);
        configureLogging();
        JetInstance jet = getJetClient();
        try {
            consumer.accept(jet);
        } finally {
            jet.shutdown();
        }
    }

    private JetInstance getJetClient() {
        return uncheckCall(() -> jetClientFn.apply(getClientConfig()));
    }

    private ClientConfig getClientConfig() throws IOException {
        ClientConfig config;
        if (isYaml()) {
            config = new YamlClientConfigBuilder(this.config).build();
        } else if (isConfigFileNotNull()) {
            config = new XmlClientConfigBuilder(this.config).build();
        } else if (addresses != null) {
            // Whole default configuration is ignored if addresses is provided.
            // This doesn't make much sense, but but addresses is deprecated, so will leave as is until it can be
            // removed in next major version
            ClientConfig c = new ClientConfig();
            c.getNetworkConfig().addAddress(addresses.toArray(new String[0]));
            c.setClusterName(clusterName);
            return c;
        } else {
            config = ConfigProvider.locateAndGetClientConfig();
        }

        if (targetsMixin.getTargets() != null) {
            config.getNetworkConfig().setAddresses(targetsMixin.getAddresses());
            config.setClusterName(targetsMixin.getClusterName());
        }

        return config;
    }

    private boolean isYaml() {
        return isConfigFileNotNull() &&
                (config.getPath().endsWith(".yaml") || config.getPath().endsWith(".yml"));
    }

    private boolean isConfigFileNotNull() {
        return config != null;
    }

    private void configureLogging() {
        JetBootstrap.configureLogging();
        Level logLevel = Level.WARNING;
        if (verbosity.isVerbose) {
            println("Verbose mode is on, setting logging level to INFO");
            logLevel = Level.INFO;
        }
        LogManager.getLogManager().getLogger("").setLevel(logLevel);
    }

    private static Job getJob(JetInstance jet, String nameOrId) {
        Job job = jet.getJob(nameOrId);
        if (job == null) {
            job = jet.getJob(Util.idFromString(nameOrId));
            if (job == null) {
                throw new JobNotFoundException("No job with name or id '" + nameOrId + "' was found");
            }
        }
        return job;
    }

    private void printf(String format, Object... objects) {
        out.printf(format + "%n", objects);
    }

    private void println(String msg) {
        out.println(msg);
    }

    /**
     * If name is longer than the {@code length}, shorten it and add an
     * asterisk so that the resulting string has {@code length} length.
     */
    private static String shorten(String name) {
        if (name.length() <= MAX_STR_LENGTH) {
            return name;
        }
        return name.substring(0, Math.min(name.length(), MAX_STR_LENGTH - 1)) + "*";
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

    private static void assertJobRunning(String name, Job job) {
        if (job.getStatus() != JobStatus.RUNNING) {
            throw new RuntimeException("Job '" + name + "' is not running. Current state: " + job.getStatus());
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
            JetBuildInfo jetBuildInfo = getBuildInfo().getJetBuildInfo();
            return new String[] {
                    "Hazelcast Jet " + jetBuildInfo.getVersion(),
                    "Revision " + jetBuildInfo.getRevision(),
                    "Build " + jetBuildInfo.getBuild()
            };
        }
    }

    public static class Verbosity {

        @Option(names = {"-v", "--verbosity"},
                description = {"Show logs from Jet client and full stack trace of errors"},
                order = 1
        )
        private boolean isVerbose;

        void merge(Verbosity other) {
            isVerbose |= other.isVerbose;
        }
    }

    public static class TargetsMixin {

        @Option(names = {"-t", "--targets"},
                description = "The cluster name and addresses to use if you want to connect to a "
                    + "cluster other than the one configured in the configuration file. " +
                        "At least one address is required. The cluster name is optional.",
                paramLabel = "[<cluster-name>@]<hostname>:<port>[,<hostname>:<port>]",
                converter = TargetsMixin.Converter.class)
        private Targets targets;

        private Targets getTargets() {
            return targets;
        }

        public String getClusterName() {
            return targets.clusterName;
        }

        public List<String> getAddresses() {
            return targets.addresses;
        }

        public void replace(TargetsMixin targets) {
            if (targets.getTargets() != null) {
                this.targets = targets.getTargets();
            }
        }

        public static class Targets {
            private String clusterName = "jet";
            private List<String> addresses = Collections.emptyList();
        }

        public static class Converter implements ITypeConverter<TargetsMixin.Targets> {
            @Override
            public Targets convert(String value) {
                Targets targets = new Targets();
                if (value == null) {
                    return targets;
                }

                String[] values;
                if (value.contains("@")) {
                    values = value.split("@");
                    targets.clusterName = values[0];
                    targets.addresses = Arrays.asList(values[1].split(","));
                } else {
                    targets.addresses = Arrays.asList(value.split(","));
                }

                return targets;
            }
        }
    }

    static class ExceptionHandler<R> extends DefaultExceptionHandler<R> {
        @Override
        public R handleExecutionException(ExecutionException ex, ParseResult parseResult) {
            // find top level command
            CommandLine cmdLine = ex.getCommandLine();
            while (cmdLine.getParent() != null) {
                cmdLine = cmdLine.getParent();
            }
            JetCommandLine jetCmd = cmdLine.getCommand();
            if (jetCmd.verbosity.isVerbose) {
                ex.printStackTrace(err());
            } else {
                err().println("ERROR: " + peel(ex.getCause()).getMessage());
                err().println();
                err().println("To see the full stack trace, re-run with the -v/--verbosity option");
            }
            if (hasExitCode()) {
                exit(exitCode());
            }
            throw ex;
        }

        static Throwable peel(Throwable e) {
            if (e instanceof InvocationTargetException) {
                return e.getCause();
            }
            return e;
        }
    }

    /**
     * A parser for SQL-like inputs. Commands are terminated with a semicolon.
     * It is mainly taken from
     *
     * @see <a href="https://github.com/julianhyde/sqlline/blob/master/src/main/java/sqlline/SqlLineParser.java">
     * SqlLineParser</a>
     * which is licensed under the BSD-3-Clause License
     */
    private static final class MultilineParser extends DefaultParser {

        private MultilineParser() {
        }

        @Override
        public ParsedLine parse(String line, int cursor, Parser.ParseContext context) throws SyntaxError {
            super.setQuoteChars(new char[]{'\'', '"'});
            super.setEofOnUnclosedQuote(true);
            stateCheck(line, cursor);
            return new ArgumentList(line, Collections.emptyList(), -1, -1,
                    cursor, "'", -1, -1);
        }

        private void stateCheck(String line, int cursor) {
            boolean containsNonCommentData = false;
            int quoteStart = -1;
            int oneLineCommentStart = -1;
            int multiLineCommentStart = -1;
            int lastNonQuoteCommentIndex = 0;

            for (int i = 0; i < line.length(); i++) {
                if (oneLineCommentStart == -1
                        && multiLineCommentStart == -1
                        && quoteStart < 0
                        && (isQuoteChar(line, i))) {
                    // Start a quote block
                    quoteStart = i;
                    containsNonCommentData = true;
                } else {
                    char currentChar = line.charAt(i);
                    if (quoteStart >= 0) {
                        // In a quote block
                        if ((line.charAt(quoteStart) == currentChar) && !isEscaped(line, i)) {
                            // End the block; arg could be empty, but that's fine
                            quoteStart = -1;
                        }
                    } else if (oneLineCommentStart == -1 && isMultiLineComment(line, i)) {
                        multiLineCommentStart = i;
                    } else if (multiLineCommentStart >= 0) {
                        if (i - multiLineCommentStart > 2
                                && currentChar == '/' && line.charAt(i - 1) == '*') {
                            // End the block; arg could be empty, but that's fine
                            multiLineCommentStart = -1;
                        }
                    } else if (oneLineCommentStart == -1 && isOneLineComment(line, i)) {
                        oneLineCommentStart = i;
                    } else if (oneLineCommentStart >= 0) {
                        if (currentChar == '\n') {
                            // End the block; arg could be empty, but that's fine
                            oneLineCommentStart = -1;
                        }
                    } else {
                        // Not in a quote or comment block
                        containsNonCommentData = true;
                        if (!Character.isWhitespace(currentChar)) {
                            lastNonQuoteCommentIndex = i;
                        }
                    }
                }
            }

            if (isEofOnEscapedNewLine() && isEscapeChar(line, line.length() - 1)) {
                throw new EOFError(-1, cursor, "Escaped new line");
            }

            if (isEofOnUnclosedQuote() && quoteStart >= 0) {
                throw new EOFError(-1, quoteStart, "Missing closing quote",
                        line.charAt(quoteStart) == '\'' ? "quote" : "dquote");
            }

            if (multiLineCommentStart != -1) {
                throw new EOFError(-1, cursor, "Missing end of comment", "**");
            }

            final int lastNonQuoteCommentIndex1 =
                    lastNonQuoteCommentIndex == line.length() - 1
                            && lastNonQuoteCommentIndex - 1 >= 0
                            ? lastNonQuoteCommentIndex - 1 : lastNonQuoteCommentIndex;
            if (containsNonCommentData
                    && !isLineFinishedWithSemicolon(
                    lastNonQuoteCommentIndex1, line)) {
                throw new EOFError(-1, cursor, "Missing semicolon (;)");
            }
        }

        /**
         * Returns whether a line (already trimmed) ends with a semicolon that
         * is not commented with one line comment.
         *
         * <p>ASSUMPTION: to have correct behavior, this method must be
         * called after quote and multi-line comments check calls, which implies that
         * there are no non-finished quotations or multi-line comments.
         *
         * @param buffer Input line to check for ending with ';'
         * @return true if the ends with non-commented ';'
         */
        private boolean isLineFinishedWithSemicolon(
                final int lastNonQuoteCommentIndex, final CharSequence buffer) {
            final String line = buffer.toString();
            boolean lineEmptyOrFinishedWithSemicolon = line.isEmpty();
            boolean requiredSemicolon = false;
            String[] oneLineComments = {"#", "--"};
            for (int i = lastNonQuoteCommentIndex; i < line.length(); i++) {
                if (';' == line.charAt(i)) {
                    lineEmptyOrFinishedWithSemicolon = true;
                    continue;
                } else if (i < line.length() - 1
                        && line.regionMatches(i, "/*", 0, "/*".length())) {
                    int nextNonCommentedChar = line.indexOf("*/", i + "/*".length());
                    // From one side there is an assumption that multi-line comment
                    // is completed, from the other side nextNonCommentedChar
                    // could be negative or less than lastNonQuoteCommentIndex
                    // in case '/*' is a part of quoting string.
                    if (nextNonCommentedChar > lastNonQuoteCommentIndex) {
                        i = nextNonCommentedChar + "*/".length();
                    }
                } else {
                    for (String oneLineCommentString : oneLineComments) {
                        if (i <= buffer.length() - oneLineCommentString.length()
                                && oneLineCommentString
                                .regionMatches(0, line, i, oneLineCommentString.length())) {
                            int nextLine = line.indexOf('\n', i + 1);
                            if (nextLine > lastNonQuoteCommentIndex) {
                                i = nextLine;
                            } else {
                                return !requiredSemicolon || lineEmptyOrFinishedWithSemicolon;
                            }
                        }
                    }
                }
                requiredSemicolon = i == line.length()
                        ? requiredSemicolon
                        : !lineEmptyOrFinishedWithSemicolon
                        || !Character.isWhitespace(line.charAt(i));
                if (requiredSemicolon) {
                    lineEmptyOrFinishedWithSemicolon = false;
                }
            }
            return !requiredSemicolon || lineEmptyOrFinishedWithSemicolon;
        }

        private boolean isMultiLineComment(final CharSequence buffer, final int pos) {
            return pos < buffer.length() - 1
                    && buffer.charAt(pos) == '/'
                    && buffer.charAt(pos + 1) == '*';
        }

        private boolean isOneLineComment(final String buffer, final int pos) {
            String[] oneLineComments = {"#", "--"};
            final int newLinePos = buffer.indexOf('\n');
            if ((newLinePos == -1 || newLinePos > pos)
                    && buffer.substring(0, pos).trim().isEmpty()) {
                for (String oneLineCommentString : oneLineComments) {
                    if (pos <= buffer.length() - oneLineCommentString.length()
                            && oneLineCommentString
                            .regionMatches(0, buffer, pos, oneLineCommentString.length())) {
                        return true;
                    }
                }
            }
            for (String oneLineCommentString : oneLineComments) {
                if (pos <= buffer.length() - oneLineCommentString.length()
                        && oneLineCommentString
                        .regionMatches(0, buffer, pos, oneLineCommentString.length())) {
                    return true;
                }
            }
            return false;
        }
    }

    private void executeSqlCmd(JetInstance jet, String command, Terminal terminal) {
        PrintWriter out = terminal.writer();
        final int colWidth = 20;
        AtomicReference<SqlResult> res = new AtomicReference<>();
        AtomicInteger rowCount = new AtomicInteger();
        ExecutorService executor = ForkJoinPool.commonPool();
        Future<?> resultFuture = executor.submit(() -> {
            try {
                res.set(jet.getSql().execute(command));
            } catch (HazelcastSqlException e) {
                out.println(e.getMessage());
                return;
            }
            if (res.get().updateCount() == -1) {
                printMetadataInfo(res.get().getRowMetadata(), colWidth, out);

                for (SqlRow row : res.get()) {
                    rowCount.getAndIncrement();
                    printRow(row, colWidth, out);
                }
                if (rowCount.get() > 0) {
                    printSeparatorLine(res.get().getRowMetadata().getColumnCount(), colWidth, out);
                }
                String prompt = new AttributedStringBuilder()
                        .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                        .append("\n")
                        .append(String.valueOf(rowCount.get()))
                        .append(" rows selected.")
                        .toAnsi();
                out.println(prompt);
            } else {
                String prompt = new AttributedStringBuilder()
                        .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                        .append("\n")
                        .append(String.valueOf(res.get().updateCount()))
                        .append(" row(s) affected")
                        .toAnsi();
                out.println(prompt);
            }
        });

        terminal.handle(Terminal.Signal.INT, signal -> resultFuture.cancel(true));

        try {
            resultFuture.get();
        } catch (CancellationException e) {
            res.get().close();
            String queryCancellationPrompt = new AttributedStringBuilder()
                    .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                    .append("\nQuery is cancelled. Until now, total ")
                    .append(String.valueOf(rowCount.get()))
                    .append(" rows received.")
                    .toAnsi();
            out.println(queryCancellationPrompt);
            out.flush();
        } catch (InterruptedException | java.util.concurrent.ExecutionException e) {
            res.get().close();
            e.printStackTrace();
        }
    }


    private static void printMetadataInfo(SqlRowMetadata metadata, int colWidth, PrintWriter out) {
        int colSize = metadata.getColumnCount();
        printSeparatorLine(colSize, colWidth, out);
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append("|");
        for (int i = 0; i < colSize; i++) {
            String colName = metadata.getColumn(i).getName();
            centralize(colWidth, builder, colName);
        }
        out.println(builder.toAnsi());
        printSeparatorLine(colSize, colWidth, out);
        out.flush();
    }

    private static void printRow(SqlRow row, int colWidth, PrintWriter out) {
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append("|");
        int colSize = row.getMetadata().getColumnCount();
        for (int i = 0; i < colSize; i++) {
            String colValue = row.getObject(i).toString();
            centralize(colWidth, builder, colValue);
        }
        out.println(builder.toAnsi());
        out.flush();
    }

    private static void centralize(int colWidth, AttributedStringBuilder builder, String colValue) {
        builder.style(AttributedStyle.DEFAULT.foreground(PRIMARY_COLOR));
        int wsLen = 0;
        if (colValue.length() < colWidth) {
            wsLen = colWidth - colValue.length();
        } else {
            colValue = colValue.substring(0, colWidth - 3) + "...";
        }

        for (int j = 0; j < wsLen / 2; j++) {
            builder.append(' ');
        }
        builder.append(colValue);
        for (int j = 0; j < wsLen / 2 + wsLen % 2; j++) {
            builder.append(' ');
        }
        builder.style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append('|');
    }

    private static void printSeparatorLine(int colSize, int colWidth, PrintWriter out) {
        AttributedStringBuilder builder = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR));
        builder.append('+');
        for (int i = 0; i < colSize; i++) {
            for (int j = 0; j < colWidth; j++) {
                builder.append('-');
            }
            builder.append('+');
        }
        out.println(builder.toAnsi());
    }

    private static class CliPrompts {
        static final String STARTING_PROMPT = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(SECONDARY_COLOR))
                .append(
                "\to   o   o   o---o o---o o     o---o   o   o---o o-o-o        o o---o o-o-o   o---o   o---o   o\n" +
                "\t|   |  / \\     /  |     |     |      / \\  |       |          | |       |     |       |   |   |\n" +
                "\to---o o---o   o   o-o   |     o     o---o o---o   |          | o-o     |     o---o   o   o   o\n" +
                "\t|   | |   |  /    |     |     |     |   |     |   |      \\   | |       |         |   |   |   |\n" +
                "\to   o o   o o---o o---o o---o o---o o   o o---o   o       o--o o---o   o     o---o   o---\\\\  o---")
                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                .append("\n\t\t\t\t\t Welcome to the Hazelcast Jet SQL Console")
                .append("\n\t\t\t\t\t Commands end with semicolon")
                .append("\n\t\t\t\t\t Type 'HELP;' to display the available commands")
                .append("\n\t\t\t\t\t Press Ctrl+C to cancel streaming queries\n\n")
    .toAnsi();
        static final String HELP_PROMPT = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                .append("AVAILABLE COMMANDS:\n\n")
                .append("CLEAR\t-\tClear the terminal screen\n")
                .append("HELP\t-\tProvide information about available commands.\n")
                .append("HISTORY\t-\tShow the command history of the current session.\n")
                .append("EXIT\t-\tExit from the SQL console.\n")
                .append("\nHints:\n")
                .append("\tPress Ctrl+C to cancel streaming queries.\n")
                .toAnsi();
        static final String EXIT_PROMPT = new AttributedStringBuilder()
                .style(AttributedStyle.BOLD.foreground(PRIMARY_COLOR))
                .append("Exiting from SQL console")
                .toAnsi();
    }
}
