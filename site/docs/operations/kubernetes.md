---
title: Jet on Kubernetes
id: kubernetes
---

The easiest way to install **Hazelcast Jet** to **Kubernetes** is using
**Helm** charts, Hazelcast Jet provides stable Helm charts for
open-source and enterprise versions also for **Hazelcast Jet Management
Center**. Hazelcast Jet also provides Kubernetes-ready **Docker** images,
these images use the **Hazelcast Kubernetes Plugin** to discover other
Hazelcast Jet members by interacting with the Kubernetes API.

# Install Hazelcast Jet using Helm

## Prerequisites

- Kubernetes 1.9+
- Helm CLI

## Installing the Chart

You can install the latest version with default configuration values
using below command:

```bash
helm install my-cluster stable/hazelcast-jet
```

This will create a cluster with the name `my-cluster` and with default
configuration values. To change various configuration options you can
use `–set key=value`:

```bash
helm install my-cluster --set cluster.memberCount=3 stable/hazelcast-jet
```

Or you can create a `values.yaml` file which contains custom
configuration options. This file may contain custom `hazelcast` and
`hazelcast-jet` yaml files in it too.

```bash
helm install my-cluster -f values.yaml stable/hazelcast-jet
```

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
helm uninstall my-release
```

The command removes all the Kubernetes components associated with the
chart and deletes the release.

## Configuration

The following table lists some of the configurable parameters of the
Hazelcast Jet chart and their default values.

| Parameter                  | Description                                                                    | Default                    |
|:---------------------------|:-------------------------------------------------------------------------------|:---------------------------|
| `image.repository`         | Hazelcast Jet Image name                                                       | `hazelcast/hazelcast-jet`  |
| `image.tag`                | Hazelcast Jet Image tag                                                        | {VERSION}                  |
| `cluster.memberCount`      | Number of Hazelcast Jet members                                                | 2                          |
| `jet.yaml.hazelcast-jet`   | Hazelcast Jet Configuration (`hazelcast-jet.yaml` embedded into `values.yaml`) | `{DEFAULT_JET_YAML}`       |
| `jet.yaml.hazelcast`       | Hazelcast IMDG Configuration (`hazelcast.yaml` embedded into `values.yaml`)    | `{DEFAULT_HAZELCAST_YAML}` |
| `managementcenter.enabled` | Turn on and off Hazelcast Jet Management Center application                    | `true`                     |


See
[stable charts repository](https://github.com/helm/charts/tree/master/stable/hazelcast-jet)
for more information and configuration options.



