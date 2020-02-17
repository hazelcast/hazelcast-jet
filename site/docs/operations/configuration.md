---
title: Configuration
id: configuration
---

Hazelcast Jet offers different ways to configure it depending on which
context you want to use Jet in.

## Configuration Files

Inside the Hazelcast Jet home folder you will find a few different
configuration files:

```text
config/hazelcast-jet.yaml
config/hazelcast.yaml
config/hazelcast-client.yaml
```

`hazelcast-jet.yaml` is used for configuring Jet specific configuration
and `hazelcast.yaml` refers to Hazelcast configuration.

Each Jet node is also a Hazelcast node, and `hazelcast.yaml` includes 
the configuration specific to clustering, discovery and so forth.

`hazelcast-client.yaml` refers to a Jet client configuration. This config
file is only used by the `jet` command line client to connect to the cluster,
but you can use it as a template for your own configuration files.

## Client Configuration

When using the Jet client as part of your application, the easiest way to
configure it using the client API:

```java
JetClientConfig config = new JetClientConfig();
config.getNetworkConfig().addAddress("server1", "server2:5702");
JetInstance jet = Jet.newJetClient(config);
```

Alternatively, you can add `hazelcast-client.yaml` to the classpath or
working directly which will be picked up automatically. The location of
the file can also be given using the `hazelcast.client.config` system
property.

A sample client YAML file is given below:

```yaml
hazelcast-client:
  # Name of the cluster to connect to. Must match the name configured on the
  # cluster members.
  cluster-name: jet
  network:
    # List of addresses for the client to try to connect to. All members of
    # a Hazelcast Jet cluster accept client connections.
    cluster-members:
      - server1:5701
      - server2:5701
  connection-strategy:
    connection-retry:
      # how long the client should keep trying connecting to the server
      cluster-connect-timeout-millis: 3000
```

## Configuration for embedded mode

When you are using an embedded Jet node, the easiest way to pass configuration
is to use programmatic configuration:

```java
JetConfig jetConfig = new JetConfig();
jetConfig.getInstanceConfig().setCooperativeThreadCount(4);
jetConfig.configureHazelcast(c -> {
    c.getNetworkConfig().setPort(5000);
});
JetInstance jet = Jet.newJetInstance(jetConfig);
```

Alternatively, you can configure Jet to load its configuration from the
classpath or working directory. By default it will search for
`hazelcast-jet.yaml` and `hazelcast.yaml` files in the classpath and
working directory, but you can control the name of the files using the
relevant system properties, `hazelcast.jet.config` and
`hazelcast.config`, respectively.

## List of configuration options

TODO, do we want to maintain a duplicate list here? or link to them?