---
title: Installation
description: Install Hazelcast Jet Enterprise
---

Hazelcast Jet Enterprise requires a license key to run. You can get a
30-day trial license from the [Hazelcast website](https://hazelcast.com/download).

## Download Hazelcast Jet

Once you have a license key, download Hazelcast Jet from [here](https://download.hazelcast.com/jet-enterprise/hazelcast-jet-enterprise-4.0.tar.gz).

Hazelcast Jet requires a minimum of JDK 8, which can be acquired from
[AdoptOpenJDK](https://adoptopenjdk.net/). Once you have the download,
unzip it to a folder which we will refer from now on as `JET_HOME`.

```bash
tar zxvf hazelcast-jet-enterprise-4.0.tar.gz
cd hazelcast-jet-enterprise-4.0
```

## Set License Key

Before you can start the node, you will need to set the license key. You
can do this by editing `<JET_HOME>/config/hazelcast.yaml`:

```yaml
hazelcast:
  cluster-name: jet
  license-key: <enter license key>
```

Once the license key is set, you can start the node using
`<JET_HOME>/bin/jet-start` per usual. The same license key can be used
on all the nodes.

It's also possible to configure the license key using `JET_LICENSE_KEY`
environment variable or `-Dhazelcast.enterprise.license.key` system
property.

When you start the node, you should the license information as below which
will list the support features.

```bash
2020-03-12 10:56:47,592  INFO [c.h.i.i.NodeExtension] [main] -  License{allowedNumberOfNodes=8,
expiryDate=02/25/2022 23:59:59, featureList=[ Management Center, Clustered JMX,
Clustered Rest, Security, Wan Replication, High Density Memory, Hot Restart,
Rolling Upgrade, Jet Management Center, Jet Lossless Recovery, Jet Rolling Job Upgrade,
Jet Enterprise, Cluster Client Filtering ],
type=Enterprise HD, companyName=null, ownerEmail=null, keyHash=NNNN, No Version Restriction}
```

## Verify Installation

You can verify the installation by submitting the `hello-word` job in
the examples folder:

```bash
bin/jet submit examples/hello-world
```

## Client Configuration

When using Hazelcast Jet Enterprise Client, there isn't any need to set
the license key as the client itself doesn't require it. A Jet client
can be created as normal using the `Jet.newJetClient` or
`Jet.bootstrappedInstance` syntax. The client binaries are not available
on Maven Central, but need to be downloaded from a repository hosted by
Hazelcast.

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
repositories {
    maven {
        url "https://repository.hazelcast.com/release/"
    }
}

compile 'com.hazelcast.jet:hazelcast-jet-enterprise:4.0'
```

<!--Maven-->

```xml
<repository>
    <id>private-repository</id>
    <name>Hazelcast Private Repository</name>
    <url>https://repository.hazelcast.com/release/</url>
</repository>

<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-enterprise</artifactId>
    <version>4.0</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Embedded Mode

When using Jet in embedded mode, there are no changes to the API used
for creating the `JetInstance`. Enterprise version is automatically
detected during startup. License key needs to be set inside the config
before node startup.

<!-- ## Install Using Docker -->

<!-- TODO -->

<!-- ## Install Using Helm -->

<!-- TODO, maybe another document? -->
