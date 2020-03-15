---
title: Management Center
description: Install Hazelcast Jet Management Center
---

Hazelcast Jet Enterprise comes with a management center which can be
used to monitor a Jet cluster and manage the lifecycle of the jobs

## Download Management Center

You can download Hazelcast Jet Management Center [here](https://download.hazelcast.com/hazelcast-jet-management-center/hazelcast-jet-management-center-4.0.tar.gz).

Once you have downloaded it, unzip it to a folder:

```bash
tar zxvf hazelcast-jet-enterprise-4.0.tar.gz
```

## Run using Docker

TODO

## Setting License Key

Like with the Jet Enterprise Server, Jet Management Center also requires
a license key. You can get a 30-day trial license from
[the Hazelcast website](https://hazelcast.com/download).

You can also run the Management Center without a license key,
but it will only work with a single node cluster. To update the license
key, edit the file `application.properties`:

```bash
# path for client configuration file (yaml or xml)
jet.clientConfig=hazelcast-client.xml
# license key for management center
jet.licenseKey=
```

## Configure Cluster IP

Hazelcast Management Center requires the address of the Hazelcast Jet
cluster to connect to it. You can configure the cluster connection
settings in `hazelcast-client.xml` file by editing the following
section:

```xml
<cluster-name>jet</cluster-name>
<network>
    <cluster-members>
        <address>127.0.0.1</address>
    </cluster-members>
</network>
```

## Start Management Center

Start the management center using the supplied script:

```bash
./jet-management-center.sh
```

The script also offers additional options during startup, which can be
viewed via:

```bash
./jet-management-center.sh --help
```

The server will by default start on port 8081 and you can browse to it
on your browser at `http://localhost:8081`. The default username and
password is `admin:admin` which you can change inside
`application.properties`.

You should be able to see the "hello-world" job running on the cluster,
if you've submitted it earlier.

## Configuring TLS

To configure Jet Management Center to use TLS, you need to edit the
`hazelcast-client.xml` file and configure these properties in the SSL
section:

```bash
<property name="protocol">TLS</property>
<property name="trustStore">/opt/hazelcast-client.truststore</property>
<property name="trustStorePassword">123456</property>
<property name="trustStoreType">JKS</property>
```
