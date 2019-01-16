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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.server.JetCommandLine.JetVersionProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.RunAll;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

@Command(
        name = "jet",
        header = "Hazelcast Jet 0.8-SNAPSHOT",
        description = "Utility for interacting with a Hazelcast Jet cluster. Global options are:%n",
        versionProvider = JetVersionProvider.class
)
public class JetCommandLine implements Callable<Void> {

    @Option(names = {"-f", "--config"},
            description = "Path to the client config XML file"
    )
    private File configXml;

    @Option(names = {"-a", "--address"},

            description = "Comma-separated list of Jet node addresses in the format <hostname>:<port>"
    )
    private String address;

    @Option(names = {"-g", "--group"},
            description = "Group name"
    )
    private String groupName;

    public static void main(String[] args) throws Exception {
        PrintStream out = System.out;
        PrintStream err = System.err;

        CommandLine cmd = new CommandLine(new JetCommandLine());

        if (args.length == 0) {
            cmd.usage(out);
        } else {
            cmd.parseWithHandlers(
                    new RunAll().useOut(out),
                    new DefaultExceptionHandler<List<Object>>().useErr(err),
                    args
            );
        }
    }

    @Override
    public Void call() throws Exception {
        return null;
    }

    @Command(description = "Submits a Jet job to the cluster")
    public void submit(
            @Option(names = {"-s", "--snapshot"},
                    paramLabel = "<snapshot>",
                    description = "Name of initial snapshot to start the job from")
                    String snapshotName,
            @Option(names = {"-n", "--name"},
                    paramLabel = "<name>",
                    description = "Name of the job")
                    String name,
            @Parameters(index = "0",
                    paramLabel = "<jar file>",
                    description = "The jar file to submit")
                    File file,
            @Parameters(index = "1..*",
                    paramLabel = "<arguments>",
                    description = "arguments to pass to the supplied jar file",
                    defaultValue = "")
                    List<String> params
    ) throws Exception {
        if (!file.exists()) {
            throw new Exception("File " + file + " could not be found.");
        }
        System.out.println("Submitting job with file " + file + " and arguments " + params);
        JetBootstrap.executeJar(file.getAbsolutePath(), params);
    }

    @Command(
            description = "Suspends a running Jet job"
    )
    public void suspend(
            @Mixin CommonOptions commonOptions,
            @Parameters(index = "0",
                    paramLabel = "<job name or id>",
                    description = "Name of the job to suspend")
                    String name
    ) throws IOException {
        runWithJet(jet -> {
            Job job = jet.getJob(name);
            if (job == null) {
                throw new RuntimeException("No job with name or id '" + name + "' was found.");
            }
            if (job.getStatus() != JobStatus.RUNNING) {
                throw new RuntimeException("Job '" + name + "' is not running. Current state: " + job.getStatus());
            }
            System.out.println("Suspending job " + job);
            job.suspend();
        });
    }


    private void runWithJet(Consumer<JetInstance> consumer) throws IOException {
        JetInstance jet = getJetInstance();
        try {
            consumer.accept(jet);
        } finally {
            jet.shutdown();
        }
    }

    private JetInstance getJetInstance() throws IOException {
        JetInstance jet;
        if (configXml != null) {
            ClientConfig config = new XmlClientConfigBuilder(configXml).build();
            jet = Jet.newJetClient(config);
        } else {
            jet = Jet.newJetClient();
        }
        return jet;
    }


    @Command(
            mixinStandardHelpOptions = true
    )
    public static class CommonOptions {

        @Option(names = {"-v", "--verbose"}, description = {"Specify verbosity"})
        protected boolean isVerbose;


    }

    public static class JetVersionProvider implements IVersionProvider {

        @Override
        public String[] getVersion() throws Exception {
            JetBuildInfo jetBuildInfo = BuildInfoProvider.getBuildInfo().getJetBuildInfo();
            return new String[]{
                    jetBuildInfo.getVersion()
            };
        }
    }
}
