# Elasticsearch Connector

A Hazelcast Jet connector for Elasticsearch (v5.6.x) for querying/indexing objects
from/to Elasticsearch.

## Getting Started

### Installing

The Elasticsearch Connector artifacts are published in the Maven repositories.

Add the following lines to your pom.xml to include it as a dependency to your project:

```
<dependency>
    <groupId>com.hazelcast.jet.contrib</groupId>
    <artifactId>elasticsearch-5</artifactId>
    <version>${version}</version>
</dependency>
```

or if you are using Gradle: 
```
compile group: 'com.hazelcast.jet.contrib', name: 'elasticsearch-5', version: ${version}
```

### Usage

#### As a Source

Elasticsearch batch source (`ElasticsearchSources.elasticsearch()`) executes
the query and retrieves the results using `scrolling`.

Following is an example pipeline which queries Elasticsearch and logs the
results:

```java
Pipeline p = Pipeline.create();

p.readFrom(ElasticsearchSources.elasticsearch("sourceName",
    () -> RestClient.builder(HttpHost.create("hostAddress")).build(),
    () -> {
        SearchRequest searchRequest = new SearchRequest("users");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(termQuery("age", 8));
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    },
    "10s",
    SearchHit::getSourceAsString,
    RestClient::close))
 .writeTo(Sinks.logger());
``` 

#### As a Sink

Elasticsearch sink (`ElasticsearchSinks.elasticsearch()`) is used to index objects from
Hazelcast Jet Pipeline to Elasticsearch.

Here is a very simple pipeline which reads out some users from Hazelcast
List and indexes them to Elasticsearch:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.list(users))
 .writeTo(ElasticsearchSinks.elasticsearch("sinkName",
    () -> RestClient.builder(HttpHost.create("hostAddress")).build(),
    BulkRequest::new,
    user -> {
        IndexRequest request = new IndexRequest(indexName, "doc", user.id);
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", user.name);
        jsonMap.put("age", user.age);
        request.source(jsonMap);
        return request;
    },
    RestClient::close));
```

### Running the tests

To run the tests run the command below: 

```
./gradlew test
```

## Authors

* **[Ali Gurbuz](https://github.com/gurbuzali)**
