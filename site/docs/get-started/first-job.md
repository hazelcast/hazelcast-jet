---
title: Writing and executing your first job
id: first-job
---

## Requirements

Hazelcast Jet is distributed as a single JAR with no other dependencies. 
It requires Java version 8 or higher to run.

### Install as a Java depedency

The easiest way to get started with Hazelcast Jet is to add is an
dependency to a Java application. Jet is packaged as just a single Jet JAR
with no dependencies that contains everything you need to get started.

Below are the Maven and Gradle snippets you can use:

<!--DOCUSAURUS_CODE_TABS-->
<!--Maven-->
```xml
<dependencies>
  <dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>{jet-version}</version>
  </dependency>
</dependencies>
```
<!--Gradle-->
```
compile 'com.hazelcast.jet:hazelcast-jet:{jet-version}'
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Write your pipeline

TODO

### Create an embedded Jet node, and run the pipeline

TODO

