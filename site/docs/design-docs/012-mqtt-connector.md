---
title: 015 - Mqtt Connector
description: Mqtt Connector (source and sink)
---

*Target Release*: 4.2

## Background

MQTT is a machine-to-machine (M2M)/”Internet of Things” connectivity
protocol. It was designed as an extremely lightweight publish/subscribe
messaging transport.

## Publish

MQTT utilizes subject-based(topics) filtering of the messages on the
broker. Each message contains a topic name, payload (binary format),
QoS (Quality of Service Level), identifier and retain-flag.

Topic Name: a simple string that is hierarchically structured with
'/' as delimiters. For example, “myhome/livingroom/temperature”.

Payload: the actual content of the message in binary format.

QoS: a number indicates the delivery guarantee of the message.

- 0, fire and forget
- 1, at least once (using acknowledgement)
- 2, exactly once (using 2-phase acknowledgement)

Identifier: a short number that uniquely identifies the messages
between the client and broker. This is relevant only for QoS levels
greater than 0. Identifier is not unique between all clients and once
the message flow is complete can be re-used.

Retain Flag: flag that defines whether the message is saved by the
broker as the last known good value for the specified topic. When a new
client subscribes to a topic, it receives the last message that is
retained on that topic.

Publishing means sending a message for a specific topic. The broker
reads, acknowledges (according to QoS) and processes the message.
Processing means determining the clients subscribed to the specified
topic and sending the message to them. The publishing client does not
know anything about the processing.

## Subscribe

To receive messages on topics of interest, the client sends a
`SUBSCRIBE` message to the broker. The message contains a list of
subscriptions which are made of a topic and a QoS level. The topic can
contain wildcards that make it possible to subscribe to a topic pattern
rather than a specific topic.

The broker sends back a return code indicating the QoS level granted
for each of the subscription. Messages published at a lower QoS will be
received at the published QoS. Messages published at a higher quality
of service will be received using the QoS specified on the subscription.
If the broker refuses a subscription, the return code indicates the
failure rather than QoS. For example, the client may have insufficient
permission to subscribe to the topic, or the topic may be malformed.

All messages sent with QoS 1 and 2 are queued for offline clients until
the client is available again. However, this queuing is only possible
if the client has a persistent session.

## Clients

We need to pick a java mqtt client for source and sink to connect to
the broker. There are several options out there:

### Paho Java Client

The Eclipse Paho project provides open-source client implementations of
MQTT protocols for various languages and [Paho Java Client](https://www.eclipse.org/paho/clients/java/)
is one of them. The client offers synchronous and asynchronous APIs, the
sync one is a wrapper to the asynchronous one. Paho Java Client does
not support MQTT 5.0 protocol yet, it is a work in progress.

If not configured explicitly, client will try to connect to the broker
using MQTT 3.1.1 protocol and if fails to connect falls back to MQTT
3.0 protocol.

The client supports:

- LWT: Last will and testament
- SSL/TLS: secure connection
- Message persistence: Client persists messages in case of an
  application crash
- Automatic reconnect: Automatically reconnects
- Offline buffering: Client buffers messages whilst offline to send
  when reconnects.
- WebSocket: Client can connect to brokers that support WebSockets
- High Availability: You can configure multiple brokers and in case of
  a failure, client tries other brokers.

It is really lightweight(240 KB), single jar without any dependencies.
We can say that it is one of the most popular clients.

### HiveMQ Client

HiveMQ Client uses `Netty` for handling networking and `RxJava` for
handling the asynchronous streaming of messages. The client provides
three distinct flavours of API: blocking, asynchronous and reactive.
HiveMQ Client supports MQTT 5.0 protocol as well as 3.1.1.

The client supports:

- LWT: Last will and testament
- SSL/TLS: secure connection
- Automatic reconnect: Automatically reconnects
- Offline buffering: Client buffers messages whilst offline to send
  when reconnects.
- WebSocket: Client can connect to brokers that support WebSockets
- Backpressure handling: ask the producers to throttle their output
  back (MQTT 5.0 feature, broker needs to have/enabled the feature)

HiveMQ Client is not lightweight compared to Paho Java client, 1.1 MB.
It has also netty and rxjava dependencies.

## MQTT Versions

1999 MQTT invention
2010 MQTT 3.1 Royalty-free release
2014 MQTT 3.1.1 OASIS standard
2016 MQTT 3.1.1 ISO standard
2018 MQTT 5 Initial release
2019 MQTT 5 OASIS standard

MQTT 5 brings new features like `Shared subscriptions` and `Time to
live` for messages and client sessions and many more. While these new
features looks promising, I couldn't find any information regarding the
adoption rate of MQTT 5. The only java client supports MQTT 5 is
`HiveMQ Client` and for other languages I've found only a single
library or none at all.
