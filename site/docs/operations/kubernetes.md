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

## Install Hazelcast Jet using Helm

### Prerequisites

- Kubernetes 1.9+
- Helm CLI

### Installing the Chart

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

### Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
helm uninstall my-release
```

The command removes all the Kubernetes components associated with the
chart and deletes the release.

### Configuration

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

## Install Hazelcast Jet without Helm

### Role Based Access Control

Hazelcast Jet provides Kubernetes-ready Docker images, these images use
the Hazelcast Kubernetes plugin to discover other Hazelcast Jet members
by interacting with the Kubernetes API. Therefore we need to create Role
Based Access Control definition, (`rbac.yaml`), with the following
content and apply it:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: default-cluster
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: view
subjects:
- kind: ServiceAccount
  name: default
  namespace: default
```
```bash
kubectly apply -f rbac.yaml
```

### ConfigMap

Then we need to configure Hazelcast Jet to use Kubernetes Discovery to
form the cluster. Create a file named `hazelcast-jet-config.yaml` with
following content and apply it. This will create a ConfigMap object.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: hazelcast-jet-configuration
data:
  hazelcast.yaml: |-
    hazelcast:
      network:
        join:
          multicast:
            enabled: false
          kubernetes:
            enabled: true
            namespace: default
            service-name: hazelcast-jet-service
```

```bash
kubectl apply -f hazelcast-jet-config.yaml
```

### StatefulSet and Service

Now we need to create a StatefulSet and a Service which defines the
container spec. You can configure the environment options and the
cluster size here. Create a file named `hazelcast-jet.yaml` with
following content and apply it.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: hazelcast-jet
  labels:
    app: hazelcast-jet
spec:
  replicas: 2
  serviceName: hazelcast-jet-service
  selector:
    matchLabels:
      app: hazelcast-jet
  template:
    metadata:
      labels:
        app: hazelcast-jet
    spec:
      containers:
      - name: hazelcast-jet
        image: hazelcast/hazelcast-jet:latest
        imagePullPolicy: IfNotPresent
        ports:
        - name: hazelcast-jet
          containerPort: 5701
        livenessProbe:
          httpGet:
            path: /hazelcast/health/node-state
            port: 5701
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          successThreshold: 1
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /hazelcast/health/node-state
            port: 5701
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 1
          successThreshold: 1
          failureThreshold: 1
        volumeMounts:
        - name: hazelcast-jet-storage
          mountPath: /data/hazelcast-jet
        env:
        - name: JAVA_OPTS
          value: "-Dhazelcast.config=/data/hazelcast-jet/hazelcast.yaml"
      volumes:
      - name: hazelcast-jet-storage
        configMap:
          name: hazelcast-jet-configuration
---
apiVersion: v1
kind: Service
metadata:
  name: hazelcast-jet-service
spec:
  selector:
    app: hazelcast-jet
  ports:
  - protocol: TCP
    port: 5701
```

```bash
kubectl apply -f hazelcast-jet.yaml
```

## Deploy Jobs

There are two different ways to submit a job to a Hazelcast Jet cluster:

- Package Job as a Docker container then let it submit itself.
- Submit Job as a JAR file from a shared `PersistentVolume` which is
  attached to a pod.

For both options you need to create a `ConfigMap` object for the client
(`hazelcast-jet-client-config.yaml`) and apply it. Make sure that the
service-name is pointing to the service-name of the cluster we have
created above.

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: hazelcast-jet-client-configuration
data:
  hazelcast-client.yaml: |-
    hazelcast-client:
      cluster-name: jet
      network:
        kubernetes:
          enabled: true
          namespace: default
          service-name: hazelcast-jet-service
```

```bash
kubectl apply -f hazelcast-jet-client-config.yaml
```

### Package the Job as a Docker Container

There are several tools to containerize your job, for example
[Jib](https://github.com/GoogleContainerTools/jib/tree/master/jib-maven-plugin).
Jib builds Docker and OCI images for Java applications. It is available
as plugins for Maven and Gradle and as a Java library. You can find a
[sample-project](https://github.com/hazelcast/hazelcast-jet-docker/tree/master/examples/kubernetes#steps-to-package-the-job-as-a-docker-container)
using Jib to containerize the Hazelcast Jet rolling-aggregate job.

After creating the image, we create a Kubernetes Job using the image and
client ConfigMap object. The client config is stored in a volume,
mounted to the container and passed as an argument to the `jet submit`
script along with the name of the JAR containing the Jet job.

Create a file named `rolling-aggregation-via-docker.yaml` and apply it.

```yaml
---
apiVersion: batch/v1
kind: Job
metadata:
  name: rolling-aggregation
spec:
  template:
    spec:
      containers:
      - name: rolling-aggregation
        image: rolling-aggregation:latest
        imagePullPolicy: IfNotPresent
        command: ["/bin/sh"]
        args: ["-c", "jet -v -f /config/hazelcast-jet/hazelcast-client.yaml submit /rolling-aggregation-jar-with-dependencies.jar"]
        volumeMounts:
          - mountPath: "/config/hazelcast-jet/"
            name: hazelcast-jet-config-storage
      volumes:
      - name: hazelcast-jet-config-storage
        configMap:
          name: hazelcast-jet-client-configuration
          items:
            - key: hazelcast-client.yaml
              path: hazelcast-client.yaml
      restartPolicy: OnFailure
```

```bash
kubectl apply -f rolling-aggregation-via-docker.yaml
```

### Submit the Job from a Shared Persistent Volume

We will need a persistent volume attached to the pods. The persistent
storage will contain job JAR files to be submitted to the cluster. There
are many different ways you can define and map volumes in Kubernetes. We
will create `hostPath` persistent volume, which mounts a file or directory
from the host node’s filesystem into the pod. See
[official documentation](https://kubernetes.io/docs/concepts/storage/volumes/)
for other types of volumes.

Create a file named `persistent-volume.yaml` and apply it:

```yaml
---
kind: PersistentVolume
apiVersion: v1
metadata:
  name: rolling-aggregation-pv
  labels:
    type: local
spec:
  storageClassName: manual
  capacity:
    storage: 2Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: "/home/docker/jars-pv"
---
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: rolling-aggregation-pv-claim
spec:
  storageClassName: manual
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```

This will create a persistent volume which will use the
`/home/docker/jars-pv` directory as persistent volume on the kubernetes
node. We will mount the volume to the pods later. So we need to put the
job JAR inside this directory.

For `minikube` below commands will create the directory and copy the job
JAR into it.

```bash
ssh docker@$(minikube ip) -i $(minikube ssh-key) 'mkdir -p ~/jars-pv'
scp -i $(minikube ssh-key) rolling-aggregation-jar-with-dependencies.jar docker@$(minikube ip):~/jars-pv/
```

Now we can create the Kubernetes Job using Hazelcast Jet image and
client ConfigMap object. The client config and the copied job JAR is
stored in respective volumes, mounted to the container and passed as an
argument to the `jet submit` script.

Create a file named `rolling-aggregation.yaml` and apply it.

```yaml
---
apiVersion: batch/v1
kind: Job
metadata:
  name: rolling-aggregation
spec:
  template:
    spec:
      containers:
      - name: rolling-aggregation
        image: hazelcast/hazelcast-jet:latest-snapshot
        imagePullPolicy: IfNotPresent
        command: ["/bin/sh"]
        args: ["-c", "jet.sh -v -f /data/hazelcast-jet/hazelcast-client.yaml submit /job-jars/rolling-aggregation-jar-with-dependencies.jar"]
        volumeMounts:
          - mountPath: "/job-jars"
            name: rolling-aggregation-pv-storage
          - mountPath: "/data/hazelcast-jet/"
            name: hazelcast-jet-config-storage
      volumes:
      - name: rolling-aggregation-pv-storage
        persistentVolumeClaim:
          claimName: rolling-aggregation-pv-claim
      - name: hazelcast-jet-config-storage
        configMap:
          name: hazelcast-jet-client-configuration
          items:
            - key: hazelcast-client.yaml
              path: hazelcast-client.yaml
      restartPolicy: OnFailure
  backoffLimit: 4
```

```bash
kubectl apply -f rolling-aggregation.yaml
```