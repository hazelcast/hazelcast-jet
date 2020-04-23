---
title: 005 - Change Data Capture (CDC) Sources
description: Change Data Capture (CDC) Sources
---

*Target Release*: 4.2

## Goal

Make Jet's CDC sources production ready: consistent with eachother in
behaviour, easy to use, reliable and performant.

## Background

In Jet 4.0 we have introduced [support for CDC sources](/blog/2020/03/02/jet-40-is-released#debezium)
based on [Debezium](https://debezium.io/).

That form however is not production ready:

* it's **inconvenient** to use: gives you huge JSON messages which you
  then have to interpret on your own, based on Debizium documentation

* native **timestamps** aren't provided (due to bugs in the code); even
  if fixed their shortcomings aren't documented (mostly that timestamps
  in events coming from database snapshots aren't really event-times)

* **performance** impact on the DBs is not explored at all, recommended
  deployment guide is not provided

* miscellaneous **shortcomings** and **inconsistencies** of the various
  connectors are not documented

These are the problems we are trying to address for the next version(s).

## Parsing Convenience

We want to help users not to have to deal with low level JSON messages
directly. In order to achieve this we:

* define standard interfaces which offer **event structure**
* offer support for mapping various event component directly into
  **POJO data objects**

### Event Structure Interfaces

The interfaces framing the general structure of change events are
quite simple, but they have proven a nice fit across all connectors.

```java
public interface ChangeEvent {

    // ...

    /**
     * Identifies the particular record or document being affected
     * by the change event.
     */
    ChangeEventKey key();

    /**
     * Describes the actual change affected on the record or ducument
     * by the change event.
     */
    ChangeEventValue value();
}
```

```java
public interface ChangeEventKey extends ChangeEventElement {
}
```

```java
public interface ChangeEventValue extends ChangeEventElement {

    // ...

    /**
     * Specifies the moment in time when the event happened. ...
     */
    long timestamp() throws ParsingException;

    /**
     * Specifies the type of change being described (insertion, delete or
     * update). ...
     */
    Operation operation() throws ParsingException;

    /**
     * Describes how the database record looked like BEFORE applying the
     * change event.
     */
    ChangeEventElement before() throws ParsingException;

    /**
     * Describes how the database record looks like AFTER the change
     * event has been applied.
     */
    ChangeEventElement after() throws ParsingException;

}
```

### Content Extraction

As can be noticed from the [event structure interfaces](#event-structure-interfaces),
all actual content can be found in the form of `ChangeEventElement`
instances.

Such an object is basically an encapsulated JSON message (part of the
original, big event message) and offers following methods to access the
data.

```java
public interface ChangeEventElement extends Serializable {

    /**
     * Maps the entire element to an instance of the specified class.
     * <p>
     * For databases providing standard JSON syntax, parsing it is based
     * on <a href="https://github.com/FasterXML/jackson-databind">Jackson Databind</a>,
     * in particular on the Jackson {@code ObjectMapper}, so the
     * parameter class needs to be annotated accordingly.
     */
    <T> T mapToObj(Class<T> clazz) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message as a child {@code ChangeEventElement}.
     */
    Optional<ChangeEventElement> getChild(String key) throws ParsingException;

    /** ... */
    Optional<String> getString(String key) throws ParsingException;

    /** ... */
    Optional<Integer> getInteger(String key) throws ParsingException;

    /** ... */
    Optional<Long> getLong(String key) throws ParsingException;

    /** ... */
    Optional<Double> getDouble(String key) throws ParsingException;

    /** ... */
    Optional<Boolean> getBoolean(String key) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message AS IS, without attempting to parse it in
     * any way. This means that it can return objects specific to the
     * parsing used by internal implementations, so Jackson classes
     * (mostly {@link JsonNode} implementations).
     * <p>
     * Should not be used normally, is intended as a fallback in case
     * regular parsing fails for some reason.
     */
    Optional<Object> getRaw(String key) throws ParsingException;

}
```

There are multiple options there, main ones being:

* fetching values and child elements via their name/key
* mapping the entire content to a POJO directly (more on that in the
  sections describing the various JSON formats used by the connectors)
* fetching low level JSON elements without attempting to interpret them,
  potentially needing to deal with objects specific to the parsing
  framework employed, so Jackson classes (mostly {@link JsonNode}
  implementations); this option is meant only as a fallback in case
  something in our parsing is faulty

### Fallback

Considering the complexity of the original JSON messages and even their
varying formats it is clear that parsing them can be error prone and
can fail in some scenarios (for example on some untested DB-connector
version combination).

To prevent a total API failure in this case each of the event structure
interfaces contains an extra `asJson()` method, which will provide the
raw JSON message the object is based on:

* `ChangeEvent.asJson()` gives you the original message from the
  connector, without anything having been done to it; if all else fails
  this will still work
* `ChangeEventElement.asJson()` gives you the JSON message of that
  particular fragment
* `ChangeEventKey.asJson()` & `ChangeEventValue.asJson()` (both of them
  are also `ChangeEventElements`) will give you the key and value parts
  of the original message

### JSON parsing

Debezium database connecters provide messages in standard JSON format
which can be parsed by any one of the well-known JSON parsing libraries
out there. For backing the implementations of our [event structure
interfaces](#event-structure-interfaces) we have chosen to use [Jackson
Databind](https://github.com/FasterXML/jackson-databind).

This is mostly an internal implementation detail, most of
``ChangeEventElement``'s methods don't reveal it. Where it becomes
visible is `getRaw()` and the object mapping method. The latter is
based on Jackson's `ObjectMapper` and the classes we pass to it expose
this fact via the annotations we have to use in them. For example:

```java
import com.fasterxml.jackson.annotation.JsonProperty;

public class Customer {

    @JsonProperty("id")
    public int id;

    @JsonProperty("first_name")
    public String firstName;

    @JsonProperty("last_name")
    public String lastName;

    @JsonProperty("email")
    public String email;

    public Customer() {
    }

    // also needs: hashCode, equals, toString
}
```

Once all these things are in place it becomes quite easy to turn the
Debezium stream of raw, complicated JSON messages into a stream of
simple data objects:

```java
StreamStage<Customer> stage = pipeline.readFrom(source)
    .withNativeTimestamps(0)
    .map(event -> {
        ChangeEventValue eventValue = event.value();
        Operation operation = eventValue.operation();
        ChangeEventElement mostRecentImage = DELETE.equals(operation) ?
                eventValue.before() : eventValue.after();
        return mostRecentImage.map(Customer.class);
    });
```

## Source Configuration

Debezium CDC connectors have a ton of configuration options and we don't
want to ask users to go to the Debezium website to explore them.

We need to provide two things. A way to **discover** and **easily use**
the config options and a way to still allow any properties to be set or
overwritten, even if for some reason we forgot or failed to handle them.

For this we are going to use builders:

```java
StreamSource<ChangeEvent> source = MySqlCdcSources.mysql(tableName)
    //set any of the essential properties we cover
    .setDatabaseAddress(mysql.getContainerIpAddress())
    .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
    .setDatabaseUser("debezium")
    .setDatabasePassword("dbz")
    .setClusterName("dbserver1")
    .setDatabaseWhitelist("inventory")
    .setTableWhitelist("inventory." + tableName)

    //set arbitrary custom property
    .setCustomProperty("database.server.id", "5401")

    //check the properties that have been set
    //construct the source
    .build();
```

Such public builders can:

* hardcode some properties we rely on
* explain what various properties do
* check rules like properties that always need to be set or can't be
  used together

## Inconsistencies

### Timestamps aren't always event times

The current version of the sources does fix timestamps, because they
haven't been working before, but beyond that timestamps have a big
weakness, which we need to at least document.

The timestamps we normally get from these sources are proper
event-times, sourced from the database change-log. The problem is with
fault tolerance. If the connector needs to be restarted it will need to
re-fetch some events (that happened after the last Jet snapshot).

It can happen in such cases, that not all these events are in the quite
limited database changelog. In such cases the connector uses a snapshot
of the database, which merges many past events into a single, current
image. Not only do events get lost for good, the timestamps in the
snapshot ones aren't event times any longer.

Luckily change events that come from snapshots can be simply identified.
They are basically insert events and their `Operation` field will not
only reflect that, but use a special type of insert value, called `SYNC`
for them. Except some, see [further inconsistencies](#snapshots-are-not-marked-in-mysql).

This problem can be mitigated for example by longer DB changelogs. They
probably aren't that big of a deal in most practical situations, but
still, they can be and we need to make that clear in our documentation.

### Snapshots are not marked in MySQL

As discussed in the [section about timestamps](#timestamps-arent-always-event-times)
there is a need to identify events that originate in database snapshots
(as opposed to database changelogs).

This is almost always possible, because such events will have `SYNC` as
their operation, except one case, MySQL which does not mark them the
same way. We could fix this in our implementation, but the fix might not
work as expected in some messages we fail to consider, causing their
parsing to fail. We don't want to do more harm then good, so for now
we'll just document this inconsistency.

## Dependencies

Regardless what convenience we add, our CDC connectors ultimately remain
Debezium based which means lots of dependencies that need to be
satisfied.

CDC sources in Jet are set up as extension modules:

* [cdc-core](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-core):
  contains all the helper classes we have defined and also allows for
  the creation of generic Debezium sources (see
  [DebeziumCdcSources](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-core/src/main/java/com/hazelcast/jet/cdc/DebeziumCdcSources.java))
* [cdc-mysql](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-mysql):
  allows for the creation of MySQL based CDC sources (see [MySqlCdcSources](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/cdc-mysql/src/main/java/com/hazelcast/jet/cdc/MySqlCdcSources.java))
* more supported databases coming in future versions

Each of these extension modules contains all the dependencies it needs.
Each of them also generates a self contained JAR in the Jet
distribution, stored in the `opt` folder of the distribution. So for
example to make members of a cluster able to work with MySQL CDC sources
all that one needs to do is to more the
`hazelcast-jet-cdc-mysql-VERSION.jar` from the member's `opt` folder to
the `lib` folder. That will take care of all the dependencies, no need
to explicitly deal with Debezium connector jars or anything else.

## Serialization

The helper classes defined in `cdc-core`, such as `ChangeEvent` and its
innards are data objects that need to travel around in Jet clusters. For
that they need to be serializable. Among the [serialization options
available in Jet](../api/serialization.md#serialization-of-data-types)
they implement the `StreamSerializer` and their `SerializerHook` gets
registered with the cluster when the `cdc-core` jar is put on the
classpath (or any of the other, database specific jars, which also
include the core).

## Distributedness

> To be explored. All Debezium connectors are based on Kafka Connect,
> which does have a [distributed mode](https://docs.confluent.io/current/connect/userguide.html#distributed-mode)
> and perhaps we can/should make use of that somehow.

## Performance

### Setup

Initial performance tests have been performed on non-production grade
hardware, but results should still be relevant:

* MySQL v5.7 running via Docker on a dedicated Ubuntu 19.10 box (quad
  core i7-6700K CPU @ 4.0GHz, 16GB memory, AMD Radeon R3 SSD drive)
* Jet CDC pipeline running in a single node Jet cluster on a MacBook
  Pro (6 core i7 CPU @ 2.6GHz, 32GB memory, Apple AP0512M SSD)

### Results

Maximum number of *sustained* record change events that could be
produced in the database (by continuously inserting into and then
deleting data from) was around **100,000 rows/second**. The Jet job
processing all the corresponding CDC events (reading them and mapping
them to user data objects) had no problems with keeping up, no lagging
behind observed.

*Peak* even handling rate of the Jet pipeline has been observed around
**200,000 events/second**. This scenario was achieved by making the
connector snapshot a very large table and inserting into the table at
peak rate while snapshotting was going on. Once the connector finished
snapshotting and switched to reading the binlog the above peak rate
seems to be what it's capable of.

*Snapshotting* seems to be pretty fast too, was observed to happen at
around **185,000 rows/second** (10 million row table finished in under
one minute).

### Database Hit

Enabling the *binlog* on the MySQL database has been observed to produce
a **15%** performance hit on the number of update/insert/delete events
it's able to process.

The effects of enabling *global transaction IDs* has not been tested.
