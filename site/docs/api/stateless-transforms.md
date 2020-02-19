---
title: Stateless Transforms
id: stateless-transforms
---

Stateless transforms could be considered the bread and butter of a data
pipeline, where they transform the input into the correct shape that is
required for further transforms. The key feature of these transforms is
that they do not have side-effects and they treat each item in
isolation.

## map

Mapping is the simplest kind of transformation is one that can be done on
each item individually. It is a stateless transform that simply applies a
function to the input item, and passes the output to the next stage.

```java
BatchStage<String> names = stage.map(name -> name.toLowerCase());
```

## filter

Similar to `map`, the `filter` operator is stateless and applies a
predicate to the input to decide whether to pass it to the output.

```java
BatchStage<String> names = stage.filter(name -> !name.isEmpty());
```

## flatMap

`flatMap` is equivalent to `map`, with the difference that instead of one
output item you can have arbitrary number of output items per input
item. The output type is a `Traverser`, which is a Jet interface that is
similar to an `Iterator`. For example, the code below will split a
sentence into individual items consisting of words:

```java
BatchStage<String> words = stage.flatMap(
    sentence -> Traversers.traverseArray(sentence.split("\\W+"))
);
```

## merge

Merges the contents of two streams into one. The item type in the
right-hand stage must be the same or a subtype of the one in the
left-hand stage. The items from both sides will be interleaved in
arbitrary order.

```java
StreamStage<Trade> tradesNewYork = tradeStream("new-york");
StreamStage<Trade> tradesTokyo = tradeStream("tokyo");
StreamStage<Trade> tradesNyAndTokyo = tradesNewYork.merge(tradesTokyo);
```

## mapUsingIMap

This transform looks up each incoming item from the corresponding
[IMap](data-structures) and the result of the lookup is combined with
the input item.

```java
StreamStage<Order> orders = p.drawFrom(Sources.kafka("orders", ..));
StreamStage<OrderDetails> details = orders.mapUsingIMap("products",
  order -> order.getProductId(),
  (order, product) -> new OrderDetails(order, product));
```

The above code can be thought of as equivalent to below, where the input
is of type `Order`

```java
public void getOrderDetails(Order order) {
    IMap<String, ProductDetails> map = jet.getMap("products");
    ProductDetails product = map.get(order.getProductId());
    return new OrderDetails(order, product);
}
```

See [Joining Static Data to a Stream](../tutorials/map-join) for a
tutorial using this operator.

## mapUsingReplicatedMap

This transform is equivalent to [mapUsingIMap](#mapUsingImap) with the
only difference that a [ReplicatedMap](data-structures) is used instead
of an `IMap`.

```java
StreamStage<Order> orders = p.drawFrom(Sources.kafka("orders", ..));
StreamStage<OrderDetails> details = orders.mapUsingReplicatedMap("products",
  order -> order.getProductId(),
  (order, product) -> new OrderDetails(order, product));
```

>With a `ReplicatedMap`, a lookup is always local compared to a standard
>`IMap`. The downside is that the data is replicated to all the nodes,
>consuming more memory in the cluster.

## mapUsingService

This tranforms takes and input, and performs a mapping using a _service_
object. The service object could represent an external HTTP-based
service, or some library which is loaded and initialized during runtime
(such as a machine learning model).

The service itself is defined through a `ServiceFactory` object. The
main difference of this operator with a simple `map` is that the service
is initialized once per job. This is useful for calling out to
heavy-weight objects which are expensive to initialize (such as HTTP
connections).

Let's imagine an HTTP service which returns details for a product and that
we have wrapped this service in a `ProductService` class:

```java
interface ProductService {
    ProductDetails getDetails(int productId);
}
```

We can then create a shared service_ factory as follows:

```java
StreamStage<Order> orders = p.drawFrom(Sources.kafka("orders", ..));
ServiceFactory<?, ProductService> productService = ServiceFactories.sharedService(ctx -> new ProductService(url));
```

Shared simply means that the factory is thread-safe, and can be called from
multiple-threads, so only one instance per node will be created.

We can then perform a lookup on this service for each incoming order:

```java
StreamStage<OrderDetails> details = orders.mapUsingService(productService,
  (service, order) -> {
      ProductDetails details = ProductDetaiservice.getDetails(order.getProductId);
      return new OrderDetails(order, details);
  }
);
```

## mapUsingServiceAsync

TODO

## hashJoin

`hashJoin` is a type of join where you have two or more inputs where all
but one of the inputs must be small enough to fit in memory. You can
consider a _primary_ input which is accompanied by one or more
_side inputs_ which are small enough to fit in memory. The side inputs
are joined to the primary input, which can be either a batch or
streaming stage. The side inputs must be batch stages.

```java
StreamStage<Order> orders = p.drawFrom(Sources.kafka("orders", ..));
BatchStage<ProductDetails>> productDetails = p.drawFrom(files("products"));
StreamStage<OrderDetails> joined = orders.hashJoin(productDetails,
        onKeys(order -> order.productId, product -> product.productId),
        (order, product) -> new OrderDetails(order, product)
);
```
