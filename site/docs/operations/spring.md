---
title: Spring Integration
id: spring
---

You can configure and start a Hazelcast Jet instance (or a Hazelcast Jet
client) declaratively using an *XML* based configuration or
programmatically using an *Annotation* based configuration.

## Declarative Configuration

Hazelcast Jet can be configured as a component in the Spring Application
Context. You can use the plain `bean` element and define individual
properties on a `JetConfig` instance, but Hazelcast Jet also provides
its own schema-based configuration which will make this much less
verbose.

### Using Default Beans Namespace

You can declare Hazelcast Jet objects using the default Spring beans
namespace. Here is an example for a Hazelcast Jet Instance declaration:

```xml
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans-2.5.xsd">
    <bean id="instance" class="com.hazelcast.jet.Jet" factory-method="newJetInstance">
        <constructor-arg>
            <bean class="com.hazelcast.jet.config.JetConfig">
                <property name="hazelcastConfig">
                    <bean class="com.hazelcast.config.Config">
                        <!-- ... -->
                    </bean>
                </property>
                <property name="instanceConfig">
                    <bean class="com.hazelcast.jet.config.InstanceConfig">
                        <property name="cooperativeThreadCount" value="2"/>
                    </bean>
                </property>
                <property name="defaultEdgeConfig">
                    <bean class="com.hazelcast.jet.config.EdgeConfig">
                        <property name="queueSize" value="2048"/>
                    </bean>
                </property>
                <property name="properties">
                    <props>
                        <prop key="foo">bar</prop>
                    </props>
                </property>
            </bean>
        </constructor-arg>
    </bean>
    <bean id="map" factory-bean="instance" factory-method="getMap">
        <constructor-arg value="my-map"/>
    </bean>
</beans>
```

### Using Jet Beans Namespace

Hazelcast Jet embeds Hazelcast IMDG thus configuration of Hazelcast Jet
(`JetConfig`) embeds the underlying Hazelcast IMDG configuration
(`Config`). Hazelcast Jet and Hazelcast IMDG provide their own Spring
config schema which enables you to configure them with their namespace
prefixes.

Here’s how your namespace and schema instance declarations may look:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:jet="http://www.hazelcast.com/schema/jet-spring"
       xmlns:hz="http://www.hazelcast.com/schema/spring"
       xsi:schemaLocation="http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans-2.5.xsd
        http://www.hazelcast.com/schema/spring
        http://www.hazelcast.com/schema/spring/hazelcast-spring-4.0.xsd
        http://www.hazelcast.com/schema/jet-spring
        http://www.hazelcast.com/schema/jet-spring/hazelcast-jet-spring-4.0.xsd">
        <!-- ... -->
 </beans>
```

#### Configuring the Hazelcast Jet Instance

Using the specified namespaces you can configure a Hazelcast Jet
instance:

```xml
<jet:instance id="instance">
    <hz:config>
        <hz:spring-aware/>
        <hz:group name="jet"/>
        <hz:network port="5701" port-auto-increment="false">
            <hz:join>
                <hz:multicast enabled="false"/>
                <hz:tcp-ip enabled="true">
                    <hz:member>127.0.0.1:5701</hz:member>
                </hz:tcp-ip>
            </hz:join>
        </hz:network>
        <hz:map name="map" backup-count="3">
        </hz:map>
    </hz:config>
    <jet:instance-config cooperative-thread-Count="2"/>
    <jet:default-edge-config queue-size="2048"/>
    <jet:properties>
        <hz:property name="foo">bar</hz:property>
    </jet:properties>
</jet:instance>
```

See that we've configured Hazelcast Jet related configuration options
(cooperative thread count, queue size for edges and a property) as well
as Hazelcast IMDG related configuration options, (group name, the join
mechanism and the `IMap` named `map` with `3` backup-count).

#### Configuring the Hazelcast Jet Client

You can configure a Hazelcast Jet client instead of an instance which
will connect to an external Hazelcast Jet cluster instead of creating an
embedded one.

```xml
<jet:client id="jet-client">
    <jet:group name="jet"/>
    <jet:network>
        <hz:member>127.0.0.1:5701</hz:member>
    </jet:network>
    <jet:spring-aware/>
</jet:client>
```

#### Additional Bean Types

You can obtain the underlying `HazelcastInstance` from the Jet instance
(or the Jet client) as a bean and use it to obtain these Hazelcast IMDG
beans:

```xml
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:jet="http://www.hazelcast.com/schema/jet-spring"
       xmlns:hz="http://www.hazelcast.com/schema/spring"
       xsi:schemaLocation="http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans-2.5.xsd
        http://www.hazelcast.com/schema/spring
        http://www.hazelcast.com/schema/spring/hazelcast-spring-4.0.xsd
        http://www.hazelcast.com/schema/jet-spring
        http://www.hazelcast.com/schema/jet-spring/hazelcast-jet-spring-4.0.xsd">

    <!-- Obtain Hazelcast IMDG instance from Hazelcast Jet instance-->
    <jet:hazelcast jet-instance-ref="jet-instance" id="hazelcast-instance"/>

    <!-- Create an IMAP named 'my-map' using Hazelcast Jet instance -->
    <jet:map instance-ref="jet-instance" name="my-map" id="my-map-bean"/>

    <!-- Create an IList named 'my-list' using Hazelcast Jet client -->
    <jet:list instance-ref="jet-client" name="my-list" id="my-list-bean"/>

    <!-- Create a MultiMap named 'my-multiMap' using Hazelcast IMDG instance -->
    <hz:multiMap id="multiMap" instance-ref="hazelcast-instance" name="my-multiMap"/>

    <!-- Create a ReplicatedMap named 'my-replicatedMap' using Hazelcast IMDG instance -->
    <hz:replicatedMap id="replicatedMap" instance-ref="hazelcast-instance" name="my-replicatedMap"/>

    <!-- Create an IQueue named 'my-queue' using Hazelcast IMDG instance -->
    <hz:queue id="queue" instance-ref="hazelcast-instance" name="my-queue"/>

    <!-- Create an ITopic named 'my-topic' using Hazelcast IMDG instance -->
    <hz:topic id="topic" instance-ref="hazelcast-instance" name="my-topic"/>

    <!-- Create an ISet named 'my-set' using Hazelcast IMDG instance -->
    <hz:set id="set" instance-ref="hazelcast-instance" name="my-set"/>

    <!-- Create an IExecutorService named 'my-executorService' using Hazelcast IMDG instance -->
    <hz:executorService id="executorService" instance-ref="hazelcast-instance" name="my-executorService"/>

    <!-- Create an IdGenerator named 'my-idGenerator' using Hazelcast IMDG instance -->
    <hz:idGenerator id="idGenerator" instance-ref="hazelcast-instance" name="my-idGenerator"/>

    <!-- Create an IAtomicLong named 'my-atomicLong' using Hazelcast IMDG instance -->
    <hz:atomicLong id="atomicLong" instance-ref="hazelcast-instance" name="my-atomicLong"/>

    <!-- Create an IAtomicReference named 'my-atomicReference' using Hazelcast IMDG instance -->
    <hz:atomicReference id="atomicReference" instance-ref="hazelcast-instance" name="my-atomicReference"/>

    <!-- Create an ISemaphore named 'my-semaphore' using Hazelcast IMDG instance -->
    <hz:semaphore id="semaphore" instance-ref="hazelcast-instance" name="my-semaphore"/>

    <!-- Create an ICountDownLatch named 'my-countDownLatch' using Hazelcast IMDG instance -->
    <hz:countDownLatch id="countDownLatch" instance-ref="hazelcast-instance" name="my-countDownLatch"/>

    <!-- Create an ILock named 'my-lock' using Hazelcast IMDG instance -->
    <hz:lock id="lock" instance-ref="hazelcast-instance" name="my-lock"/>
</beans>
```

Hazelcast Jet also supports `lazy-init`, `scope` and `depends-on` bean
attributes.

```xml
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:jet="http://www.hazelcast.com/schema/jet-spring"
       xsi:schemaLocation="http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans-2.5.xsd
        http://www.hazelcast.com/schema/jet-spring
        http://www.hazelcast.com/schema/jet-spring/hazelcast-jet-spring-4.0.xsd">
    <jet:instance id="instance" lazy-init="true" scope="singleton">
    <!-- ... -->
    </jet:instance>
    <jet:client id="client" scope="prototype" depends-on="instance">
    <!-- ... -->
    </jet:client>
</beans>
```

## Annotation-Based Configuration

Annotation-Based Configuration does not require any XML definition.
Simply create a configuration class annotated with `@Configuration` and
provide a `JetInstance` (either a member or client) as a bean by
annotating the method with `@Bean`.

```java
@Configuration
public class AppConfig {

    @Bean
    public JetInstance instance() {
        // You can configure Hazelcast Jet isntance programmatically
        JetConfig jetConfig = new JetConfig();
        return Jet.newJetInstance(jetConfig);
    }

    @Bean
    public JetInstance client() {
        // You can configure Hazelcast Jet client programmatically
        ClientConfig clientConfig = new ClientConfig();
        return Jet.newJetClient(clientConfig);
    }
}
```
