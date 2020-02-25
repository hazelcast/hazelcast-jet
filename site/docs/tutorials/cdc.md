---
title: Change Data Capture
id: cdc
---

**Change data capture** refers to the process of **observing changes
made to a database** and extracting them in a form usable by other
systems, for the purposes of replication, analysis and many many more.

Change Data Capture is especially important to Jet, because it allows
for the **integration with legacy systems**. Database changes form a
stream of events which can be efficiently processed by Jet.

Implementation of CDC in Jet is based on
[Debezium](https://debezium.io/), which is an open source distributed
platform for change data capture. It provides Kafka Connect compatible
CDC connectors for a
[variety of popular databases](https://debezium.io/documentation/reference/1.0/connectors/index.html)
.

The [Kafka Connect API](http://kafka.apache.org/documentation.html#connect)
is an interface developed for Kafka, that simplifies and automates the
integration of a new data source (or sink) with your Kafka cluster.
Since version 4.0 Jet includes a generic Kafka Connect Source, thus
making the integration of Debezium's connectors a simple matter of
configuration:

<!--DOCUSAURUS_CODE_TABS-->
<!--MongoDB-->

```java
Configuration configuration = Configuration
        .create()
        .with("name", "mongodb-inventory-connector")
        .with("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
        /* begin connector properties */
        .with("mongodb.hosts", "rs0/" + mongo.getContainerIpAddress() + ":"
                + mongo.getMappedPort(MongoDBContainer.MONGODB_PORT))
        .with("mongodb.name", "fullfillment")
        .with("mongodb.user", "debezium")
        .with("mongodb.password", "dbz")
        .with("mongodb.members.auto.discover", "false")
        .with("collection.whitelist", "inventory.*")
        .with("database.history.hazelcast.list.name", "test")
        .build();

Pipeline pipeline = Pipeline.create();
pipeline.readFrom(DebeziumSources.cdc(configuration))
        .withoutTimestamps()
        .map(record -> Values.convertToString(record.valueSchema(), record.value()))
        .writeTo(Sinks.logger());

JobConfig jobConfig = new JobConfig();
jobConfig.addJarsInZip("/path/to/debezium-connector-mongodb.zip");

JetInstance jet = Jet.bootstrappedInstance();
Job job = jet.newJob(pipeline, jobConfig);
job.join();
```

<!--MySQL-->

```java
Configuration configuration = Configuration
        .create()
        .with("name", "mysql-inventory-connector")
        .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
        /* begin connector properties */
        .with("database.hostname", mysql.getContainerIpAddress())
        .with("database.port", mysql.getMappedPort(MYSQL_PORT))
        .with("database.user", "debezium")
        .with("database.password", "dbz")
        .with("database.server.id", "184054")
        .with("database.server.name", "dbserver1")
        .with("database.whitelist", "inventory")
        .with("database.history.hazelcast.list.name", "test")
        .build();

Pipeline pipeline = Pipeline.create();
pipeline.readFrom(DebeziumSources.cdc(configuration))
        .withoutTimestamps()
        .map(record -> Values.convertToString(record.valueSchema(), record.value()))
        .writeTo(Sinks.logger());

JobConfig jobConfig = new JobConfig();
jobConfig.addJarsInZip("/path/to/debezium-connector-mysql.zip");

JetInstance jet = Jet.bootstrappedInstance();
Job job = jet.newJob(pipeline, jobConfig);
job.join();
```

<!--PostgreSQL-->

```java
Configuration configuration = Configuration
        .create()
        .with("name", "postgres-inventory-connector")
        .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        /* begin connector properties */
        .with("database.hostname", postgres.getContainerIpAddress())
        .with("database.port", postgres.getMappedPort(POSTGRESQL_PORT))
        .with("database.user", "postgres")
        .with("database.password", "postgres")
        .with("database.dbname", "postgres")
        .with("database.server.name", "dbserver1")
        .with("schema.whitelist", "inventory")
        .with("database.history.hazelcast.list.name", "test")
        .with("tasks.max", "1")
        .build();

Pipeline pipeline = Pipeline.create();
pipeline.readFrom(DebeziumSources.cdc(configuration))
        .withoutTimestamps()
        .map(record -> Values.convertToString(record.valueSchema(), record.value()))
        .writeTo(Sinks.logger());

JobConfig jobConfig = new JobConfig();
jobConfig.addJarsInZip("/path/to/debezium-connector-postgres.zip");

JetInstance jet = Jet.bootstrappedInstance();
Job job = jet.newJob(pipeline, jobConfig);
job.join();
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Dependencies

To run the above sample code you will need following libraries external to
Jet (latest versions available at the time of writing):

<!--DOCUSAURUS_CODE_TABS-->
<!--Maven-->

```xml
<dependencies>
  <dependency>
      <groupId>com.hazelcast.jet.contrib</groupId>
      <artifactId>debezium</artifactId>
      <version>0.1</version>
  </dependency>
</dependencies>
```

<!--Gradle-->

```bash
compile 'com.hazelcast.jet.contrib:debezium:0.1'
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Uploading Connectors to the Job Classpath

Since we are instantiating external Kafka Connect Connectors on the Jet
runtime, we need to be able to access those classes. Connectors are
usually **shipped as a ZIP file**. The JAR files from inside the ZIP
archive can be uploaded to the Jet classpath via the `addJarsInZip`
 method of the `JobConfig` class.

To find the connector archives:

* figure out what version of Debezium the Jet's debezium library
  depends on; at the time of writing version 0.1 of the library
  depends on [Debezium 1.0](https://search.maven.org/artifact/io.debezium/debezium-core/1.0.0.Final/jar)
  , but this information can be simply checked at any time by looking at
   the Jet debezium library's
  [transitive dependencies on Maven](https://search.maven.org/artifact/com.hazelcast.jet.contrib/debezium/0.1/jar)
* go to the [Releases section](https://debezium.io/releases/) on the
  Debezium website
* find the version you need, currently [1.0](https://debezium.io/releases/1.0/)
* go to [Maven artifacts](https://search.maven.org/search?q=g:io.debezium%20and%20v:1.0.0.Final*)
* download "plugin.zip" for the connector you need

## Events

When a database client queries a database, it uses the database’s current
schema. However, the database schema can be changed at any time, which
means that the connector must know what the schema looked like at the
time each insert, update, or delete operation is recorded. For this
reason the events coming out of the Debezium connectors are
**self-contained**.

Each event has a **key** and a **value**. Every message key and value
has two parts: a **schema** and **payload**. The schema describes the
structure of the payload, while the payload contains the actual data.

Furthermore each connector emits events in a different schema so there
is no single format shared among connectors. The specifics of working
with each connector's data can be found in their respective
[documentation](https://debezium.io/documentation/reference/1.0/connectors/index.html)
(latest stable version available at the time of writing).

## Interpreting Events

Even though events are connector specific and working with them is not
always straightforward, Debezium offers some assistance with
[deserializing them](https://debezium.io/documentation/reference/1.0/configuration/serdes.html)
.

Below you can see an example Jet Pipeline, which:

* takes CDC data from a MySQL database (same configuration as
  previous example)
* filters out all events except ones from the "customers" table (same
  effect could be achieved by whitelisting the table in the connector
  config)
* formats them into JSON strings
* deserializes the JSON strings into `Customer` objects (simple POJOs)
* stores the latest `Customer` object for each customer ID in an
  [IMap](https://docs.hazelcast.org/docs/latest-dev/javadoc/com/hazelcast/map/IMap.html)

```java
Pipeline pipeline = Pipeline.create();
pipeline.readFrom(DebeziumSources.cdc(configuration))
        .withoutTimestamps()
        .filter(r -> r.topic().equals("dbserver1.inventory.customers"))
        .map(record -> Values.convertToString(record.valueSchema(), record.value()))
        .mapUsingService(
                ServiceFactories.nonSharedService(cntx -> {
                    Serde<Customer> serde = DebeziumSerdes.payloadJson(Customer.class);
                    serde.configure(Collections.singletonMap("from.field", "after"), false);
                    return serde;
                }),
                (serde, json) -> {
                    Customer customer = serde.deserializer()
                                .deserialize("topic", json.getBytes());
                    return entry(customer.id, customer);
                })
        .writeTo(Sinks.map("customers"));
```

The **Customers** class used to build the deserializer is quite simple:

```java
private static final class Customer implements Serializable {
    public int id;

    @JsonProperty("first_name")
    public String firstName;

    @JsonProperty("last_name")
    public String lastName;

    public String email;

    public Customer() {
    }

    public Customer(int id, String firstName, String lastName, String email) {
        super();
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
    }

    @Override
    public int hashCode() {
        return Objects.hash(email, firstName, id, lastName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Customer other = (Customer) obj;
        return id == other.id
                && Objects.equals(firstName, other.firstName)
                && Objects.equals(lastName, other.lastName)
                && Objects.equals(email, other.email);
    }
}
```

## Snapshots

When a connector gets started up and not all database change-logs still
exist (typically the case when the database has been running for some
time), an initial snapshot of the database’s current state can be taken.

Debezium connectors will do so, if configured accordingly, and will
provide the contents of the snapshot in the form of events. Then they
will transition to providing the normal, log based events. This will be
done in a consistent way, no changes happening during the serving of the
snapshot will be lost.

Unfortunately the specifics of snapshotting differ from connector to
connector so their individual
[documentation](https://debezium.io/documentation/reference/1.0/connectors/index.html)
needs to be consulted.
