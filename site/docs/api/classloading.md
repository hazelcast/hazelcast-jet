---
title: Class and Resource Deployment
description: How to make sure that a processing job has all the resources it needs, when it's submitted to a Jet cluster.
---

>Under construction!

Intro: explain why it's necessary to add classes to a job when sending it
to a cluster, and how it works.

## Submit as a JAR

How to build an uber JAR and submit it as a job.

> Inspiration for writing proper content,  sample build scripts that
> build fat jars:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
    id 'java'
}

group 'org.example'
version '1.0-SNAPSHOT'

repositories.mavenCentral()

dependencies {
    compileOnly 'com.hazelcast.jet:hazelcast-jet:4.0'
    compile 'com.danielflower.apprunner:javasysmon:0.3.5.1'
}

jar {
    from {
        configurations.compile.collect { it.isDirectory() ? it : zipTree(it) }
    }
}
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>custom-sink-tutorial</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.target>1.8</maven.compiler.target>
        <maven.compiler.source>1.8</maven.compiler.source>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.hazelcast.jet</groupId>
            <artifactId>hazelcast-jet</artifactId>
            <version>4.0</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>com.danielflower.apprunner</groupId>
            <artifactId>javasysmon</artifactId>
            <version>0.3.5.1</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <executions>
                    <execution>
                        <id>distro-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                        <configuration>
                            <descriptorRefs>
                                <descriptorRef>jar-with-dependencies</descriptorRef>
                            </descriptorRefs>
                            <tarLongFileMode>posix</tarLongFileMode>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

> Some things to point out:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

Configuration of Jet dependency is `compileOnly` (as opposed to
`compile`) so that it won't be included in the fat jar.

Other dependencies can be set up with the `compile` configuration.

Extra lines in `jar` block needed to put external dependency code into
the jar.

To build the fat jar call:

```bash
gradle build
```

The produced jar will be located in the `build/libs` folder of the
project.

<!--Maven-->

Scope of the Jet dependency is `provided` so that it won't be included
in the fat jar.

Other dependencies can be left on the default scope.

Extra configuration for the `maven-assembly-plugin` needed to put
external dependency code into the jar (actually to create an extra
jar with dependencies included).

To build the fat jar call:

```bash
mvn package
```

The produced jar will be located in the `target` folder or the project.

<!--END_DOCUSAURUS_CODE_TABS-->

## Adding to Classpath

How to add things directly to class path.

Describe what must be on classpath (i.e. serializers, map loader etc)

## Attaching Classes

How to attach classes manually and send them using Jet client.

## Attaching additional files

Describe to how to attach files, and how to access them inside a job.

## User Code Deployment

Describe how this feature can be used to deploy additional classes (but maybe
it's best to avoid for now as it's WIP)
