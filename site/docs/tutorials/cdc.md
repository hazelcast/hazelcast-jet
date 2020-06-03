---
title: Change Data Capture
description: How to monitor Change Data Capture data from databases in Jet.
---

**Change data capture** refers to the process of **observing changes
made to a database** and extracting them in a form usable by other
systems, for the purposes of replication, analysis and many more.

Change Data Capture is especially important to Jet, because it allows
for the **streaming of changes from databases**, which can be
efficiently processed by Jet.

Implementation of CDC in Jet is based on
[Debezium](https://debezium.io/), which is an open source distributed
platform for change data capture.

Let's see an example, how to process change events in Jet, from a
database of your choice:

* [MySQL](cdc/cdc-mysql.md)
* [Postgres](cdc/cdc-postgres.md)
